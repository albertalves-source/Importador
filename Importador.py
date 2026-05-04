import streamlit as st
import pandas as pd
from datetime import datetime
import base64
import json
import time
import requests
import re
import io
import PyPDF2
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed

class JSONParser:
    @staticmethod
    def extrair_json_puro(texto):
        try:
            match = re.search(r'\{.*\}', texto, re.DOTALL)
            if match:
                return match.group(0)
            return texto
        except:
            return texto

# ==========================================
# MOTOR OFFLINE - TÉCNICA HÍBRIDA (V10.2)
# ==========================================
def extrair_dados_pdf_offline(file_name, file_bytes, cnpj_destino_usuario):
    try:
        leitor = PyPDF2.PdfReader(io.BytesIO(file_bytes))
        texto_bruto = ""
        for pagina in leitor.pages:
            extraido = pagina.extract_text()
            if extraido:
                texto_bruto += extraido + " "
                
        if len(texto_bruto.strip()) < 50:
            return None, "PDF ilegível ou imagem. Use o modo IA."
            
        texto_limpo = re.sub(r'\s+', ' ', texto_bruto).upper()
        texto_limpo = ''.join(c for c in unicodedata.normalize('NFD', texto_limpo) if unicodedata.category(c) != 'Mn')
        texto_denso = texto_limpo.replace(' ', '')
        
        dados = {
            "doc": None, 
            "serie": "1", 
            "data": None, 
            "cnpj_forn": None, 
            "valor_total": None, 
            "acumulador": "1", # Valor padrão
            "file_name": file_name
        }
        
        cnpj_alvo_limpo = "".join(filter(str.isdigit, str(cnpj_destino_usuario)))
        todos_numeros_longos = re.findall(r'\d{14}|\d{11}', texto_denso)
        
        for doc in list(dict.fromkeys(todos_numeros_longos)):
            if doc != cnpj_alvo_limpo and doc != "00000000000000" and not doc.startswith("25155"):
                dados["cnpj_forn"] = doc
                break
        
        if not dados["cnpj_forn"]:
            dados["cnpj_forn"] = todos_numeros_longos[0] if todos_numeros_longos else "00000000000000"

        doc_str = None
        padroes_doc = [
            r"NUMERO DA NFS-E[^\d]{0,50}?0*(\d+)",
            r"NUMERO DA NOTA[^\d]{0,50}?0*(\d+)",
            r"NFS-E[^\d]{0,30}?0*(\d+)",
            r"DANF-?E[^\d]{0,30}?0*(\d+)",
            r"NUMERO[^\d]{0,50}?0*(\d+)",
            r"NF-?E?\s*[:.-]?\s*0*(\d+)",
        ]
        
        for padrao in padroes_doc:
            matches = re.findall(padrao, texto_limpo)
            for m in matches:
                if m not in ['2024', '2025', '2026', '2027', '0'] and 1 <= len(m) <= 15:
                    doc_str = m
                    break
            if doc_str: break

        if doc_str: dados["doc"] = int(doc_str)
        data_match = re.search(r"(\d{2}/\d{2}/\d{4})", texto_denso)
        if data_match: dados["data"] = data_match.group(1)

        regex_dinheiro = r"\b\d{1,10}(?:[.,]\d{3})*[.,]\d{2}\b"
        todos_brutos = re.findall(regex_dinheiro, texto_limpo)
        valores_float = []
        
        for bruto in todos_brutos:
            v_str = re.sub(r'[.,]', '', bruto[:-3]) + '.' + bruto[-2:] if len(bruto)>3 else bruto
            try:
                v_f = float(v_str)
                if 0.50 < v_f < 99000000.0: valores_float.append(v_f)
            except: continue
        
        if valores_float: dados["valor_total"] = max(valores_float)
            
        if dados["doc"] and dados["valor_total"]:
            return dados, None
        return None, "Dados insuficientes no PDF."
    except Exception as e:
        return None, f"Erro: {str(e)}"

# ==========================================
# MOTOR IA (GEMINI)
# ==========================================
def call_gemini_api_direct(file_name, file_bytes, model_name, api_key):
    if not api_key: return None, "Chave de API ausente."
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"
    base64_data = base64.b64encode(file_bytes).decode('utf-8')
    prompt = """Extraia os dados desta Nota Fiscal em formato JSON. 
    Campos: {"doc": 123, "serie": "1", "data": "01/01/2026", "cnpj_forn": "00000000000000", "valor_total": 1500.0, "acumulador": "1"}"""
    payload = {"contents": [{"parts": [{"text": prompt}, {"inlineData": {"mimeType": "application/pdf", "data": base64_data}}]}]}
    
    try:
        response = requests.post(url, json=payload, timeout=90)
        if response.status_code == 200:
            raw_text = response.json()['candidates'][0]['content']['parts'][0]['text']
            data = json.loads(JSONParser.extrair_json_puro(raw_text))
            data['file_name'] = file_name
            return data, None
        return None, f"Erro API: {response.status_code}"
    except Exception as e:
        return None, f"Falha na IA: {str(e)}"

# --- FUNÇÕES DOMÍNIO ---
def limpar_cnpj(v): return "".join(filter(str.isdigit, str(v or "")))
def formatar_valor(v): return f"{float(v):.2f}".replace('.', ',')

def gerar_registro_0000(cnpj): return f"|0000|{limpar_cnpj(cnpj)}|"

def gerar_registro_1000(nf, obs=""):
    dt = nf.get('data', '')
    acum = str(nf.get('acumulador', '1'))
    campos = ["1000", "1", limpar_cnpj(nf.get('cnpj_forn', '')), "", "1", acum, "", str(nf.get('doc', '') or ""), "1", "", dt, dt, formatar_valor(nf.get('valor_total', 0)), "", obs, "C", "", "", "", "", "", "", "", "", "E"]
    return "|" + "|".join(campos) + "|" + "|" * 70

def gerar_registro_1020(nf):
    v = nf.get('valor_total', 0)
    return f"|1020|1||{formatar_valor(v)}|0,00|0,00|0,00|0,00|0,00|0,00|{formatar_valor(v)}||||"

def gerar_registro_1300(nf, obs=""):
    return f"|1300|{nf.get('data', '')}|55|5|{formatar_valor(nf.get('valor_total', 0))}|1|{obs}|SISTEMA|"

# --- INTERFACE ---
st.set_page_config(page_title="Domínio Automator v10.4", layout="wide")
st.title("⚡ Domínio Automator - V10.4")

# Inicialização de estados
if 'notas_finalizadas' not in st.session_state: st.session_state.notas_finalizadas = []
if 'falhas' not in st.session_state: st.session_state.falhas = {}

with st.sidebar:
    st.header("⚙️ Configurações Gerais")
    metodo = st.radio("Tecnologia de Leitura:", ["1. MODO RÁPIDO (Offline)", "2. MODO LENTO (IA)"])
    modo_offline = "RÁPIDO" in metodo
    
    if not modo_offline:
        api_input = st.text_input("Gemini API Key", type="password")
        model_choice = st.selectbox("Modelo:", ["gemini-2.0-flash", "gemini-1.5-flash"], index=0)
    
    st.markdown("---")
    cnpj_alvo = st.text_input("CNPJ Empresa Destino", value="40633348000130")
    texto_obs = st.text_input("Observação Padrão", value="IMPORTACAO AUTOMATICA")
    
    if st.button("🗑️ Limpar Todos os Dados"):
        st.session_state.notas_finalizadas = []
        st.session_state.falhas = {}
        st.rerun()

# --- SEPARAÇÃO DAS FUNCIONALIDADES EM ABAS ---
tab1, tab2 = st.tabs(["📄 Importação Simples", "🔍 Confronto com Mês Anterior"])

# ---------------------------------------------------------
# ABA 1: IMPORTAÇÃO SIMPLES
# ---------------------------------------------------------
with tab1:
    st.subheader("Processamento Direto de Notas")
    arquivos_simples = st.file_uploader("Arraste os PDFs para importação direta", type="pdf", accept_multiple_files=True, key="up_simples")
    
    if arquivos_simples:
        nomes_proc = [n['file_name'] for n in st.session_state.notas_finalizadas]
        pendentes = [f for f in arquivos_simples if f.name not in nomes_proc]
        
        if pendentes and st.button("🚀 Processar Notas (Simples)", key="btn_simples"):
            pbar = st.progress(0)
            for idx, f in enumerate(pendentes):
                f_bytes = f.read()
                res, erro = extrair_dados_pdf_offline(f.name, f_bytes, cnpj_alvo) if modo_offline else call_gemini_api_direct(f.name, f_bytes, model_choice, api_input)
                if res: st.session_state.notas_finalizadas.append(res)
                else: st.session_state.falhas[f.name] = erro
                pbar.progress((idx + 1) / len(pendentes))
            st.rerun()

# ---------------------------------------------------------
# ABA 2: CONFRONTO COM MÊS ANTERIOR
# ---------------------------------------------------------
with tab2:
    st.subheader("Processamento com Cruzamento de Dados")
    col_a, col_b = st.columns(2)
    
    with col_a:
        arquivos_confronto = st.file_uploader("1. PDFs das Notas", type="pdf", accept_multiple_files=True, key="up_conf_pdf")
    with col_b:
        arquivo_excel = st.file_uploader("2. Excel do Mês Anterior", type=["xlsx", "xls"], key="up_conf_excel")
        st.caption("Colunas esperadas: 'CNPJ' e 'Acumulador'")

    if arquivos_confronto and st.button("🚀 Processar e Confrontar", key="btn_confronto"):
        # 1. Processar PDFs
        nomes_proc = [n['file_name'] for n in st.session_state.notas_finalizadas]
        pendentes = [f for f in arquivos_confronto if f.name not in nomes_proc]
        
        if pendentes:
            for f in pendentes:
                f_bytes = f.read()
                res, erro = extrair_dados_pdf_offline(f.name, f_bytes, cnpj_alvo) if modo_offline else call_gemini_api_direct(f.name, f_bytes, model_choice, api_input)
                if res: st.session_state.notas_finalizadas.append(res)
                else: st.session_state.falhas[f.name] = erro
        
        # 2. Aplicar Confronto se o Excel existir
        if arquivo_excel:
            try:
                df_ref = pd.read_excel(arquivo_excel)
                df_ref.columns = [c.upper().strip() for c in df_ref.columns]
                
                if 'CNPJ' in df_ref.columns and 'ACUMULADOR' in df_ref.columns:
                    df_ref['CNPJ_CLEAN'] = df_ref['CNPJ'].apply(lambda x: limpar_cnpj(str(x)))
                    de_para = dict(zip(df_ref['CNPJ_CLEAN'], df_ref['ACUMULADOR']))
                    
                    for nota in st.session_state.notas_finalizadas:
                        cnpj_f = limpar_cnpj(nota['cnpj_forn'])
                        if cnpj_f in de_para:
                            nota['acumulador'] = str(de_para[cnpj_f])
                    st.success("✅ Confronto realizado com sucesso!")
                else:
                    st.error("Excel sem as colunas 'CNPJ' e 'ACUMULADOR'.")
            except Exception as e:
                st.error(f"Erro no Excel: {e}")
        st.rerun()

# --- ÁREA COMUM DE REVISÃO E DOWNLOAD (Sempre visível se houver notas) ---
if st.session_state.notas_finalizadas:
    st.divider()
    st.subheader("📝 Revisão Final e Ajustes")
    st.info("Ajuste os códigos na coluna 'Acumulador' se necessário antes de baixar.")
    
    df_preview = pd.DataFrame(st.session_state.notas_finalizadas)
    cols = ['doc', 'cnpj_forn', 'acumulador', 'valor_total', 'data', 'file_name']
    
    df_editado = st.data_editor(
        df_preview[cols],
        column_config={
            "acumulador": st.column_config.TextColumn("Acumulador", help="Código no Domínio"),
            "doc": st.column_config.NumberColumn("Nº Nota", disabled=True),
            "valor_total": st.column_config.NumberColumn("Valor R$", format="%.2f", disabled=True),
            "cnpj_forn": st.column_config.TextColumn("CNPJ Fornecedor", disabled=True)
        },
        use_container_width=True,
        hide_index=True,
        key="editor_final"
    )

    if st.button("💾 GERAR ARQUIVO PARA DOMÍNIO", use_container_width=True):
        buffer = [gerar_registro_0000(cnpj_alvo)]
        for _, nf in df_editado.iterrows():
            buffer.append(gerar_registro_1000(nf, texto_obs))
            buffer.append(gerar_registro_1020(nf))
            buffer.append(gerar_registro_1300(nf, texto_obs))
        
        st.download_button(
            label="📥 Baixar Arquivo .TXT",
            data="\r\n".join(buffer),
            file_name=f"importacao_{datetime.now().strftime('%d%m_%H%M')}.txt",
            mime="text/plain",
            use_container_width=True
        )

# Exibição de Erros
if st.session_state.falhas:
    with st.expander("⚠️ Falhas de Leitura"):
        st.table([{"Arquivo": k, "Erro": v} for k, v in st.session_state.falhas.items()])

st.divider()
st.caption("Domínio Automator v10.4 - Fluxos de trabalho independentes.")
