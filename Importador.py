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
# MOTOR DE EXTRAÇÃO PDF (ATUALIZADO V11.1)
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
            return None, "PDF é uma IMAGEM/SCAN. Para este arquivo, use o MODO LENTO (IA)."
            
        texto_limpo = re.sub(r'\s+', ' ', texto_bruto).upper()
        texto_limpo = ''.join(c for c in unicodedata.normalize('NFD', texto_limpo) if unicodedata.category(c) != 'Mn')
        texto_denso = texto_limpo.replace(' ', '')
        
        dados = {
            "doc": None, 
            "serie": "1", 
            "data": None, 
            "cnpj_forn": None, 
            "valor_total": None, 
            "acumulador": "1", 
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
            r"NUMERO DA NFS-E[^\d]{0,50}?0*(\d{1,15})",
            r"NUMERO DA NOTA[^\d]{0,50}?0*(\d{1,15})",
            r"NFS-E[^\d]{0,30}?0*(\d{1,15})",
            r"DANF-?E[^\d]{0,30}?0*(\d{1,15})",
            r"NUMERO[^\d]{0,50}?0*(\d{1,15})",
            r"NF-?E?\s*[:.-]?\s*0*(\d{1,15})",
            r"N[O0°º]\s*[:.-]?\s*0*(\d{1,15})",
            r"NOTA[^\d]{0,30}?0*(\d{1,15})"
        ]
        
        for padrao in padroes_doc:
            matches = re.findall(padrao, texto_limpo)
            validos = [m for m in matches if m not in ['2024', '2025', '2026', '2027', '0'] and len(m) > 0]
            if validos:
                doc_str = validos[0]
                break

        if not doc_str:
            nums_arq = re.findall(r'\d+', file_name)
            validos_arq = [n for n in nums_arq if n not in ['2024', '2025', '2026', '2027'] and int(n) > 0]
            if validos_arq:
                doc_str = max(validos_arq, key=len)
                if len(doc_str) < 1: doc_str = validos_arq[0]

        if doc_str: 
            try: dados["doc"] = int(doc_str)
            except: dados["doc"] = 1
        else:
            dados["doc"] = 1

        data_match = re.search(r"(\d{2}/\d{2}/\d{4})", texto_denso)
        if data_match: dados["data"] = data_match.group(1)
        else: dados["data"] = datetime.now().strftime("%d/%m/%Y")

        valores_float = []
        todos_brutos = re.findall(r"(\d{1,10}(?:[.,]\d{3})*[.,]\d{2})", texto_limpo)
        if not todos_brutos:
            todos_brutos = re.findall(r"(\d{1,10}(?:[.,]\d{3})*[.,]\d{2})", texto_denso)

        for bruto in todos_brutos:
            v_str = re.sub(r'[.,]', '', bruto[:-3]) + '.' + bruto[-2:] if len(bruto)>3 else bruto
            try:
                v_f = float(v_str)
                if 0.50 < v_f < 99000000.0 and v_f not in [2024.0, 2025.0, 2026.0, 2027.0]: 
                    valores_float.append(v_f)
            except: continue
        
        if valores_float: dados["valor_total"] = max(valores_float)
            
        if dados["valor_total"] is not None:
            return dados, None
            
        return None, "Faltou Valor na leitura offline. Tente o MODO IA."
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

# --- FUNÇÕES AUXILIARES ---
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

def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Dados_Notas')
    return output.getvalue()

# --- INTERFACE ---
st.set_page_config(page_title="Domínio Automator v11.1", layout="wide")
st.title("⚡ Domínio Automator - V11.1")

if 'notas_extraidas' not in st.session_state: st.session_state.notas_extraidas = []
if 'falhas' not in st.session_state: st.session_state.falhas = {}

with st.sidebar:
    st.header("🔀 Ferramentas (Separadas)")
    ferramenta = st.radio("Selecione o que deseja fazer:", [
        "📄 1. Importar Notas (De PDF para Excel/TXT)",
        "📊 2. Confronto e Edição (De Excel para Excel/TXT)"
    ])
    
    st.markdown("---")
    st.header("⚙️ Configurações Gerais")
    metodo = st.radio("Tecnologia de Leitura:", ["1. MODO RÁPIDO (Offline)", "2. MODO LENTO (IA)"])
    modo_offline = "RÁPIDO" in metodo
    
    if not modo_offline:
        api_input = st.text_input("Gemini API Key", type="password")
        model_choice = st.selectbox("Modelo:", ["gemini-2.0-flash", "gemini-1.5-flash"], index=0)
    
    cnpj_alvo = st.text_input("CNPJ Empresa Destino", value="40633348000130")
    texto_obs = st.text_input("Observação Padrão", value="IMPORTACAO AUTOMATICA")
    
    if st.button("🗑️ Limpar Memória do Sistema"):
        st.session_state.notas_extraidas = []
        st.session_state.falhas = {}
        st.rerun()

# =========================================================
# MÓDULO 1: APENAS IMPORTAÇÃO DE PDFS
# =========================================================
if "1. Importar Notas" in ferramenta:
    st.subheader("📄 Módulo de Extração de PDFs")
    st.write("Faça o upload dos PDFs. O sistema lerá os dados e permitirá que você baixe a planilha Excel ou o arquivo TXT.")
    
    arquivos_pdf = st.file_uploader("Arraste os PDFs das notas aqui", type="pdf", accept_multiple_files=True)
    
    if arquivos_pdf and st.button("🚀 Processar PDFs"):
        st.session_state.notas_extraidas = []
        st.session_state.falhas = {}
        pbar = st.progress(0)
        
        for idx, f in enumerate(arquivos_pdf):
            f_bytes = f.read()
            res, erro = extrair_dados_pdf_offline(f.name, f_bytes, cnpj_alvo) if modo_offline else call_gemini_api_direct(f.name, f_bytes, model_choice, api_input)
            if res: st.session_state.notas_extraidas.append(res)
            else: st.session_state.falhas[f.name] = erro
            pbar.progress((idx + 1) / len(arquivos_pdf))
            
    if st.session_state.notas_extraidas:
        st.success(f"{len(st.session_state.notas_extraidas)} notas extraídas com sucesso!")
        df_extr = pd.DataFrame(st.session_state.notas_extraidas)
        st.dataframe(df_extr[['doc', 'cnpj_forn', 'valor_total', 'data', 'file_name']], use_container_width=True)
        
        col1, col2 = st.columns(2)
        with col1:
            st.download_button(
                label="📥 Baixar Planilha (Excel)",
                data=to_excel(df_extr),
                file_name=f"notas_extraidas_{datetime.now().strftime('%d%m')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        with col2:
            buffer = [gerar_registro_0000(cnpj_alvo)]
            for _, nf in df_extr.iterrows():
                buffer.append(gerar_registro_1000(nf.to_dict(), texto_obs))
                buffer.append(gerar_registro_1020(nf.to_dict()))
                buffer.append(gerar_registro_1300(nf.to_dict(), texto_obs))
            st.download_button(
                label="📥 Baixar Arquivo Domínio (TXT)",
                data="\r\n".join(buffer),
                file_name=f"importacao_{datetime.now().strftime('%d%m_%H%M')}.txt",
                mime="text/plain",
                use_container_width=True
            )

    if st.session_state.falhas:
        with st.expander("⚠️ Ver PDFs que falharam"):
            st.table([{"Arquivo": k, "Erro": v} for k, v in st.session_state.falhas.items()])

# =========================================================
# MÓDULO 2: CONFRONTO EM EXCEL
# =========================================================
elif "2. Confronto e Edição" in ferramenta:
    st.subheader("📊 Módulo de Confronto e Edição via Excel")
    st.write("Cruze a planilha das notas atuais com a planilha do mês anterior. Baixe o resultado para alterar no Excel, ou gere o TXT.")
    
    colA, colB = st.columns(2)
    with colA:
        excel_atual = st.file_uploader("1. Excel das Notas do Mês (Baixado no Passo 1)", type=["xlsx", "xls", "csv"])
    with colB:
        excel_base = st.file_uploader("2. Excel do Mês Anterior (Para Confronto - Opcional)", type=["xlsx", "xls", "csv"])
        
    if excel_atual:
        try:
            # Correção para aceitar CSV (já que os arquivos subidos tinham esse formato também)
            if excel_atual.name.endswith('.csv'):
                df_atual = pd.read_csv(excel_atual)
            else:
                df_atual = pd.read_excel(excel_atual)
            
            if excel_base:
                if excel_base.name.endswith('.csv'):
                    df_ref = pd.read_csv(excel_base)
                else:
                    df_ref = pd.read_excel(excel_base)
                
                # CORREÇÃO APLICADA AQUI: str(c) converte qualquer nome de coluna (inclusive números) para texto
                df_ref.columns = [str(c).upper().strip() for c in df_ref.columns]
                
                if 'CNPJ' in df_ref.columns and 'ACUMULADOR' in df_ref.columns:
                    df_ref['CNPJ_CLEAN'] = df_ref['CNPJ'].apply(lambda x: limpar_cnpj(str(x)))
                    de_para = dict(zip(df_ref['CNPJ_CLEAN'], df_ref['ACUMULADOR']))
                    
                    def vincular_acum(cnpj):
                        return str(de_para.get(limpar_cnpj(cnpj), '1'))
                        
                    if 'cnpj_forn' in df_atual.columns:
                        df_atual['acumulador'] = df_atual['cnpj_forn'].apply(vincular_acum)
                        st.success("✅ Cruzamento com o mês anterior realizado com sucesso!")
                else:
                    st.warning("O Excel anterior não tem as colunas 'CNPJ' e 'ACUMULADOR' bem definidas.")
            
            st.write("Pré-visualização da Planilha:")
            st.dataframe(df_atual, use_container_width=True)
            
            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    label="📥 Baixar Excel Cruzado (Para alterar no seu PC)",
                    data=to_excel(df_atual),
                    file_name=f"notas_confrontadas_{datetime.now().strftime('%d%m')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            with col2:
                if st.button("💾 Gerar TXT para o Domínio a partir desta planilha", use_container_width=True):
                    buffer = [gerar_registro_0000(cnpj_alvo)]
                    for _, nf in df_atual.iterrows():
                        nf_dict = nf.to_dict()
                        buffer.append(gerar_registro_1000(nf_dict, texto_obs))
                        buffer.append(gerar_registro_1020(nf_dict))
                        buffer.append(gerar_registro_1300(nf_dict, texto_obs))
                    st.download_button(
                        label="📥 Descarregar Arquivo Domínio (TXT)",
                        data="\r\n".join(buffer),
                        file_name=f"importacao_final_{datetime.now().strftime('%H%M')}.txt",
                        mime="text/plain",
                        use_container_width=True
                    )
        except Exception as e:
            st.error(f"Erro ao processar as planilhas: {e}")
