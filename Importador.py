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
# MOTOR OFFLINE - TÉCNICA HÍBRIDA (V9.9)
# ==========================================
def extrair_dados_pdf_offline(file_name, file_bytes, cnpj_destino_usuario):
    """
    Lê o PDF com precisão máxima. Fatiamento de Salsicha Numérica,
    Busca de Identidade Cega e Suporte a layouts verticais.
    """
    try:
        leitor = PyPDF2.PdfReader(io.BytesIO(file_bytes))
        texto_bruto = ""
        for pagina in leitor.pages:
            extraido = pagina.extract_text()
            if extraido:
                texto_bruto += extraido + " "
                
        if len(texto_bruto.strip()) < 50:
            return None, "PDF é uma imagem escaneada ou ilegível. Utilize o modo de Inteligência Artificial."
            
        # TEXTO LIMPO: Mantém espaços, padroniza maiúsculas e remove acentos
        texto_limpo = re.sub(r'\s+', ' ', texto_bruto).upper()
        texto_limpo = ''.join(c for c in unicodedata.normalize('NFD', texto_limpo) if unicodedata.category(c) != 'Mn')
        
        # TEXTO DENSO: Remove espaços para padrões rígidos imunes a colunas
        texto_denso = texto_limpo.replace(' ', '')
        
        dados = {"doc": None, "serie": "1", "data": None, "cnpj_forn": None, "valor_total": None, "aliq_icms": 0.0, "file_name": file_name}
        
        # ---------------------------------------------------------
        # 1. CNPJ / CPF Emitente (Identidade Cega V9.9)
        # ---------------------------------------------------------
        # Captura todos os documentos (CNPJ ou CPF) ignorando espaços/lixo
        regex_elastica = r"(\d{2,3}\s*[\.\s-]?\s*\d{3}\s*[\.\s-]?\s*\d{3}\s*[\/\s-]?\s*\d{4}\s*[\-\s-]?\s*\d{2})"
        possiveis_docs = re.findall(regex_elastica, texto_limpo)
        
        docs_limpos = []
        for d in possiveis_docs:
            limpo = "".join(filter(str.isdigit, d))
            if len(limpo) in [11, 14]: docs_limpos.append(limpo)
            
        cnpj_alvo_limpo = "".join(filter(str.isdigit, str(cnpj_destino_usuario)))
        
        # Filtro: O fornecedor é quem NÃO é o alvo e NÃO é apenas zeros
        for doc in docs_limpos:
            if doc != cnpj_alvo_limpo and doc != "00000000000000":
                dados["cnpj_forn"] = doc
                break
        
        # Fallback se nada foi encontrado
        if not dados["cnpj_forn"]:
            if docs_limpos: dados["cnpj_forn"] = docs_limpos[0]
            else: dados["cnpj_forn"] = "00000000000000"

        # ---------------------------------------------------------
        # 2. NÚMERO DO DOCUMENTO (Suporte a Acentos e Verticalidade)
        # ---------------------------------------------------------
        doc_str = None
        # Adicionado 'NUMERO' com e sem acento e 'N[Oº°]'
        padroes_doc = [
            r"NUMERO DA NFS-E[^\d]{0,30}?0*(\d+)",
            r"NUMERO DA NOTA[^\d]{0,30}?0*(\d+)",
            r"NFS-E[^\d]{0,20}?0*(\d+)",
            r"NUMERO[^\d]{0,30}?0*(\d+)",
            r"NF[^\d]{0,20}?0*(\d+)",
            r"DANFE[^\d]{0,20}?0*(\d+)",
            r"FATURA[^\d]{0,20}?0*(\d+)"
        ]
        
        for padrao in padroes_doc:
            matches = re.findall(padrao, texto_limpo)
            for m in matches:
                # Impede capturar anos ou IDs de 44 dígitos
                if m not in ['2024', '2025', '2026', '2027'] and 1 <= len(m) <= 15:
                    doc_str = m
                    break
            if doc_str: break

        # Truque do Nome do Arquivo
        if not doc_str and file_name:
            match_nome = re.search(r'(?:NF|NOTA|FATURA)[_ -]*[Nn]?[Oo]?[_ -]*0*(\d+)', file_name.upper())
            if match_nome: doc_str = match_nome.group(1)

        if doc_str: dados["doc"] = int(doc_str)

        # ---------------------------------------------------------
        # 3. DATA
        # ---------------------------------------------------------
        data_match = re.search(r"(\d{2}/\d{2}/\d{4})", texto_denso)
        if data_match: dados["data"] = data_match.group(1)

        # ---------------------------------------------------------
        # 4. VALOR TOTAL (Fatiador Anti-Salsicha V9.9)
        # ---------------------------------------------------------
        # Captura valores. Se houver fusão (ex: 100,000,00), cortamos na 1ª parte válida.
        regex_dinheiro = r"(\d{1,10}(?:[.,]\d{3})*[.,]\d{2})"
        todos_brutos = re.findall(regex_dinheiro, texto_limpo)
        valores_float = []
        
        for bruto in todos_brutos:
            # Proteção: Se o PDF colou valores (ex: 233.333,330,00), pegamos só os primeiros 233.333,33
            # Fazemos isso verificando se existem múltiplos separadores decimais
            partes_virgula = bruto.split(',')
            if len(partes_virgula) > 2: # Tem mais de uma vírgula! Salsicha detetada.
                corrigido = partes_virgula[0] + "," + partes_virgula[1][:2]
            else:
                corrigido = bruto
                
            v_str = re.sub(r'[.,]', '', corrigido[:-3]) + '.' + corrigido[-2:] if len(corrigido)>3 and corrigido[-3] in '.,' else re.sub(r'[.,]', '', corrigido)
            v_f = float(v_str)
            
            # Filtro de sanidade: Ignora anos e lixo de sistema
            if 0.50 < v_f < 999999999.0 and v_f not in [2024.0, 2025.0, 2026.0]:
                valores_float.append(v_f)
        
        if valores_float:
            # Em notas de serviço, o maior valor é quase sempre o total.
            dados["valor_total"] = max(valores_float)
            
        # Validação Final
        if dados["doc"] is not None and dados["valor_total"] is not None:
            return dados, None
        else:
            return None, f"Incompleto -> Doc:{dados.get('doc')} | R$:{dados.get('valor_total')}"
            
    except Exception as e:
        return None, f"Erro Técnico: {str(e)}"

# ==========================================
# MOTOR IA (GEMINI) - FALLBACK
# ==========================================
DEFAULT_KEY = "" 

def call_gemini_api_direct(file_name, file_bytes, model_name, api_key, status_placeholder):
    if not api_key: return None, "Chave de API ausente."
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key.strip()}"
    base64_data = base64.b64encode(file_bytes).decode('utf-8')
    prompt = """Extraia os dados desta Nota Fiscal de Serviço em JSON.
    Campos: {"doc": 123, "serie": "1", "data": "01/01/2026", "cnpj_forn": "00000000000000", "valor_total": 1500.0, "aliq_icms": 0.0}"""
    payload = {"contents": [{"parts": [{"text": prompt}, {"inlineData": {"mimeType": "application/pdf", "data": base64_data}}]}]}
    
    try:
        response = requests.post(url, json=payload, timeout=60)
        if response.status_code == 200:
            raw_text = response.json()['candidates'][0]['content']['parts'][0]['text']
            json_str = JSONParser.extrair_json_puro(raw_text)
            data = json.loads(json_str)
            data['file_name'] = file_name
            return data, None
        return None, f"Erro IA HTTP {response.status_code}"
    except Exception as e:
        return None, str(e)

# --- FUNÇÕES DOMÍNIO ---
def limpar_cnpj(v): return "".join(filter(str.isdigit, str(v or "")))
def formatar_valor(v): return f"{float(v):.2f}".replace('.', ',')

def gerar_registro_0000(cnpj): return f"|0000|{limpar_cnpj(cnpj)}|"
def gerar_registro_1000(nf, obs=""):
    dt = nf.get('data', '')
    campos = ["1000", "1", limpar_cnpj(nf.get('cnpj_forn', '')), "", "1", "1102", "", str(nf.get('doc', '')), "1", "", dt, dt, formatar_valor(nf.get('valor_total', 0)), "", obs, "C", "", "", "", "", "", "", "", "", "E"]
    return "|" + "|".join(campos) + "|" + "|" * 70
def gerar_registro_1020(nf):
    v = nf.get('valor_total', 0)
    return f"|1020|1||{formatar_valor(v)}|0,00|0,00|0,00|0,00|0,00|0,00|{formatar_valor(v)}||||"
def gerar_registro_1300(nf, obs=""):
    return f"|1300|{nf.get('data', '')}|55|5|{formatar_valor(nf.get('valor_total', 0))}|1|{obs}|SISTEMA|"

# --- INTERFACE ---
st.set_page_config(page_title="Domínio Automator v9.9", layout="wide")
st.title("⚡ Domínio Automator - V9.9 (Motor de Precisão)")

with st.sidebar:
    st.header("⚙️ Configurações")
    metodo = st.radio("Método:", ["1. MODO RÁPIDO (Offline)", "2. MODO LENTO (IA)"])
    modo_offline = "RÁPIDO" in metodo
    
    if not modo_offline:
        api_input = st.text_input("Gemini Key", type="password")
    
    st.markdown("---")
    cnpj_alvo = st.text_input("CNPJ Empresa Destino", value="40633348000130")
    texto_obs = st.text_input("Observação", value="IMPORTACAO AUTOMATICA")

if 'notas_finalizadas' not in st.session_state: st.session_state.notas_finalizadas = {}
if 'falhas' not in st.session_state: st.session_state.falhas = {}

arquivos = st.file_uploader("Arraste os PDFs aqui", type="pdf", accept_multiple_files=True)

if arquivos:
    pendentes = [f for f in arquivos if f.name not in st.session_state.notas_finalizadas]
    
    if st.button("🚀 PROCESSAR NOTAS"):
        pbar = st.progress(0)
        
        for idx, f in enumerate(pendentes):
            f_bytes = f.read()
            
            if modo_offline:
                res, erro = extrair_dados_pdf_offline(f.name, f_bytes, cnpj_alvo)
            else:
                res, erro = call_gemini_api_direct(f.name, f_bytes, "gemini-1.5-flash", api_input, None)
            
            if res:
                st.session_state.notas_finalizadas[f.name] = res
                if f.name in st.session_state.falhas: del st.session_state.falhas[f.name]
            else:
                st.session_state.falhas[f.name] = erro
            
            pbar.progress((idx + 1) / len(pendentes))
        
        st.rerun()

if st.session_state.falhas:
    with st.expander("⚠️ Falhas", expanded=True):
        st.table(pd.DataFrame([{"Arquivo": k, "Motivo": v} for k, v in st.session_state.falhas.items()]))

if st.session_state.notas_finalizadas:
    st.subheader("✅ Notas Lidas")
    df_ok = pd.DataFrame(list(st.session_state.notas_finalizadas.values()))
    st.dataframe(df_ok[['doc', 'cnpj_forn', 'valor_total', 'data', 'file_name']], use_container_width=True)
    
    buffer = [gerar_registro_0000(cnpj_alvo)]
    for nf in st.session_state.notas_finalizadas.values():
        buffer.append(gerar_registro_1000(nf, texto_obs))
        buffer.append(gerar_registro_1020(nf))
        buffer.append(gerar_registro_1300(nf, texto_obs))
    
    st.download_button("📥 Baixar Arquivo Domínio", "\r\n".join(buffer), f"importacao_{datetime.now().strftime('%H%M')}.txt")

st.divider()
st.caption("v9.9 - Fatiador de Decimais e Identidade Cega.")
