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
# MOTOR DE EXTRAÇÃO PDF (V10.2)
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

        # BUSCA DE DOCUMENTO MAIS AGRESSIVA
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

        # FALLBACK PARA O NOME DO ARQUIVO (Se não achar o número no texto, procura no nome do PDF)
        if not doc_str:
            nums_arq = re.findall(r'\d+', file_name)
            validos_arq = [n for n in nums_arq if n not in ['2024', '2025', '2026', '2027'] and int(n) > 0]
            if validos_arq:
                # Pega o maior número do título (útil para chaves WebISS grandes) ou o primeiro achado
                doc_str = max(validos_arq, key=len)
                if len(doc_str) < 1: doc_str = validos_arq[0]

        if doc_str: 
            try: dados["doc"] = int(doc_str)
            except: dados["doc"] = 1
        else:
            dados["doc"] = 1 # Fallback para não dar erro

        # DATA (Com fallback para hoje)
        data_match = re.search(r"(\d{2}/\d{2}/\d{4})", texto_denso)
        if data_match: dados["data"] = data_match.group(1)
        else: dados["data"] = datetime.now().strftime("%d/%m/%Y")

        # BUSCA DE VALOR MAIS FLEXÍVEL
        valores_float = []
        # Busca 1: Tenta no texto com espaços
        todos_brutos = re.findall(r"(\d{1,10}(?:[.,]\d{3})*[.,]\d{2})", texto_limpo)
        # Busca 2: Se falhar, tenta no texto denso (sem espaços)
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
            
        # VALIDAÇÃO FINAL RELAXADA (Só rejeita se faltar o Valor)
        if dados["valor_total"] is not None:
            return dados, None
            
        return None, f"Faltou Valor na leitura offline. Tente o MODO IA."
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
st.set_page_config(page_title="Domínio Automator v10.5", layout="wide")
st.title("⚡ Domínio Automator - V10.5")

# Inicialização de estados
if 'notas_extraidas' not in st.session_state: st.session_state.notas_extraidas = []
if 'falhas' not in st.session_state: st.session_state.falhas = {}

with st.sidebar:
    st.header("⚙️ Configurações")
    metodo = st.radio("Tecnologia de Leitura:", ["1. MODO RÁPIDO (Offline)", "2. MODO LENTO (IA)"])
    modo_offline = "RÁPIDO" in metodo
    
    if not modo_offline:
        api_input = st.text_input("Gemini API Key", type="password")
        model_choice = st.selectbox("Modelo:", ["gemini-2.0-flash", "gemini-1.5-flash"], index=0)
    
    st.markdown("---")
    cnpj_alvo = st.text_input("CNPJ Empresa Destino", value="40633348000130")
    texto_obs = st.text_input("Observação Padrão", value="IMPORTACAO AUTOMATICA")
    
    if st.button("🗑️ Limpar Todos os Dados"):
        st.session_state.notas_extraidas = []
        st.session_state.falhas = {}
        st.rerun()

tab1, tab2 = st.tabs(["📊 1. Extrair PDF p/ Excel", "📥 2. Gerar TXT via Excel"])

# ---------------------------------------------------------
# ABA 1: EXTRAÇÃO (PDF -> EXCEL)
# ---------------------------------------------------------
with tab1:
    st.subheader("Passo 1: Transformar PDFs em Planilha")
    
    col_up1, col_up2 = st.columns(2)
    with col_up1:
        arquivos_pdf = st.file_uploader("1. Carregar PDFs das Notas", type="pdf", accept_multiple_files=True, key="pdf_extr")
    with col_up2:
        arquivo_ref = st.file_uploader("2. Carregar Excel Mês Anterior (Opcional)", type=["xlsx", "xls"], key="ref_extr")
        st.caption("O sistema tentará preencher o acumulador automaticamente via Excel.")

    if arquivos_pdf and st.button("🚀 Iniciar Extração", key="btn_extr"):
        st.session_state.notas_extraidas = [] # Limpa para novo processamento
        pbar = st.progress(0)
        for idx, f in enumerate(arquivos_pdf):
            f_bytes = f.read()
            res, erro = extrair_dados_pdf_offline(f.name, f_bytes, cnpj_alvo) if modo_offline else call_gemini_api_direct(f.name, f_bytes, model_choice, api_input)
            if res: st.session_state.notas_extraidas.append(res)
            else: st.session_state.falhas[f.name] = erro
            pbar.progress((idx + 1) / len(arquivos_pdf))
        
        if st.session_state.notas_extraidas:
            df_final = pd.DataFrame(st.session_state.notas_extraidas)
            
            # Cruzamento com Excel se fornecido
            if arquivo_ref:
                try:
                    df_ref = pd.read_excel(arquivo_ref)
                    df_ref.columns = [c.upper().strip() for c in df_ref.columns]
                    if 'CNPJ' in df_ref.columns and 'ACUMULADOR' in df_ref.columns:
                        df_ref['CNPJ_CLEAN'] = df_ref['CNPJ'].apply(lambda x: limpar_cnpj(str(x)))
                        de_para = dict(zip(df_ref['CNPJ_CLEAN'], df_ref['ACUMULADOR']))
                        
                        def vincular_acum(cnpj):
                            c = limpar_cnpj(cnpj)
                            return str(de_para.get(c, '1'))
                        
                        df_final['acumulador'] = df_final['cnpj_forn'].apply(vincular_acum)
                        st.success("✅ Confronto realizado no processamento!")
                except Exception as e:
                    st.error(f"Erro ao cruzar com Excel: {e}")

            st.dataframe(df_final, use_container_width=True)
            
            # Botão de Download do Excel
            excel_data = to_excel(df_final)
            st.download_button(
                label="📥 Baixar Planilha para Ajustar no Excel",
                data=excel_data,
                file_name=f"notas_para_ajuste_{datetime.now().strftime('%d%m')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

# ---------------------------------------------------------
# ABA 2: IMPORTAÇÃO (EXCEL -> TXT)
# ---------------------------------------------------------
with tab2:
    st.subheader("Passo 2: Gerar Ficheiro para o Domínio")
    st.write("Após fazer o cruzamento e as alterações no seu Excel, carregue a planilha final aqui.")
    
    arquivo_final_excel = st.file_uploader("Carregar Planilha Ajustada (Excel)", type=["xlsx", "xls"], key="up_final")
    
    if arquivo_final_excel:
        try:
            df_ajustado = pd.read_excel(arquivo_final_excel)
            st.info("Revisão rápida dos dados carregados:")
            
            # Editor final apenas para conferência rápida
            df_conferencia = st.data_editor(df_ajustado, use_container_width=True, hide_index=True)
            
            if st.button("💾 Gerar Arquivo .TXT para Domínio", key="btn_gerar_txt"):
                buffer = [gerar_registro_0000(cnpj_alvo)]
                for _, nf in df_conferencia.iterrows():
                    # Garante que os campos existem
                    nf_dict = nf.to_dict()
                    buffer.append(gerar_registro_1000(nf_dict, texto_obs))
                    buffer.append(gerar_registro_1020(nf_dict))
                    buffer.append(gerar_registro_1300(nf_dict, texto_obs))
                
                st.download_button(
                    label="📥 Descarregar Arquivo Domínio",
                    data="\r\n".join(buffer),
                    file_name=f"importacao_dominio_{datetime.now().strftime('%H%M')}.txt",
                    mime="text/plain",
                    use_container_width=True
                )
        except Exception as e:
            st.error(f"Erro ao ler a planilha ajustada: {e}")

# Exibição de Erros de Extração
if st.session_state.falhas:
    with st.expander("⚠️ Falhas na Extração de PDFs"):
        st.table([{"Arquivo": k, "Erro": v} for k, v in st.session_state.falhas.items()])

st.divider()
st.caption("Domínio Automator v10.5 - Foco em Fluxo via Excel.")
