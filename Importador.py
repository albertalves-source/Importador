import streamlit as st
import pandas as pd
from datetime import datetime
import base64
import json
import time
import requests
import re
import io
import PyPDF2 # Biblioteca super rápida para ler texto de PDFs
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
# MOTOR DE EXTRAÇÃO OFFLINE (CÓDIGO PURO)
# ==========================================
def extrair_dados_pdf_offline(file_name, file_bytes):
    """
    Lê o PDF instantaneamente sem usar IA.
    Configurado para o Padrão Nacional (DANFSe v1.0) e Padrão São Paulo.
    """
    try:
        # Abre o PDF e extrai o texto bruto
        leitor = PyPDF2.PdfReader(io.BytesIO(file_bytes))
        texto = ""
        for pagina in leitor.pages:
            texto += pagina.extract_text() + "\n"
            
        dados = {
            "doc": None,
            "serie": "1",
            "data": None,
            "cnpj_forn": None,
            "valor_total": None,
            "aliq_icms": 0.0,
            "file_name": file_name
        }

        # 1. TENTA LER COMO "PADRÃO SÃO PAULO CAPITAL"
        if "PREFEITURA DO MUNICÍPIO DE SÃO PAULO" in texto or "SECRETARIA MUNICIPAL DA FAZENDA" in texto:
            doc_match = re.search(r"Número da Nota[\s\n]+(\d+)", texto)
            data_match = re.search(r"Data e Hora de Emissão[\s\n]+(\d{2}/\d{2}/\d{4})", texto)
            cnpj_match = re.search(r"PRESTADOR DE SERVIÇOS.*?CPF/CNPJ:\s*([\d\.\-\/]+)", texto, re.DOTALL)
            valor_match = re.search(r"VALOR TOTAL DO SERVIÇO\s*=\s*R\$\s*([\d\.,]+)", texto)
            
            if doc_match: dados["doc"] = int(doc_match.group(1)) # Remove zeros à esquerda
            if data_match: dados["data"] = data_match.group(1)
            if cnpj_match: dados["cnpj_forn"] = re.sub(r"\D", "", cnpj_match.group(1))
            if valor_match:
                v_str = valor_match.group(1).replace('.', '').replace(',', '.')
                dados["valor_total"] = float(v_str)

        # 2. TENTA LER COMO "PADRÃO NACIONAL (DANFSe v1.0)"
        elif "DANFSe v1.0" in texto or "Documento Auxiliar da NFS-e" in texto:
            doc_match = re.search(r"Número da NFS-e[\s\n]+(\d+)", texto)
            data_match = re.search(r"Competência da NFS-e[\s\n]+(\d{2}/\d{2}/\d{4})", texto)
            if not data_match:
                data_match = re.search(r"Data e Hora da emissão.*?[\s\n]+(\d{2}/\d{2}/\d{4})", texto)
            
            # Procura CNPJ apenas do Emitente/Prestador
            cnpj_match = re.search(r"EMITENTE DA NFS-e.*?CNPJ/CPF/NIF[\s\n]+([\d\.\-\/]+)", texto, re.DOTALL)
            
            # Valor do Serviço
            valor_match = re.search(r"Valor do Serviço[\s\n]+R\$\s*([\d\.,]+)", texto)
            if not valor_match:
                valor_match = re.search(r"Valor Líquido da NFS-e[\s\n]+R\$\s*([\d\.,]+)", texto)
                
            if doc_match: dados["doc"] = int(doc_match.group(1))
            if data_match: dados["data"] = data_match.group(1)
            if cnpj_match: dados["cnpj_forn"] = re.sub(r"\D", "", cnpj_match.group(1))
            if valor_match: 
                v_str = valor_match.group(1).replace('.', '').replace(',', '.')
                dados["valor_total"] = float(v_str)

        # VALIDAÇÃO FINAL
        if dados["doc"] and dados["cnpj_forn"] and dados["valor_total"] is not None:
            return dados, None
        else:
            return None, "Layout do PDF desconhecido ou não suportado no modo offline."
            
    except Exception as e:
        return None, f"Erro ao ler PDF offline: {str(e)}"

# ==========================================
# MOTOR DE EXTRAÇÃO IA (GEMINI) - FALLBACK
# ==========================================
DEFAULT_KEY = "AIzaSyB_mDR97ABexRXVSUQkxd_bgYjL_xHKaw8"

def call_gemini_api_direct(file_name, file_bytes, model_name, api_key, status_placeholder):
    if not api_key: return None, "Chave de API ausente."
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key.strip()}"
    base64_data = base64.b64encode(file_bytes).decode('utf-8')
    prompt = """
    Extraia os dados desta Nota Fiscal de Serviço (NFS-e). Responda APENAS com um objeto JSON válido.
    Campos exatos: {"doc": 162, "serie": "1", "data": "01/03/2026", "cnpj_forn": "14243715000180", "valor_total": 16018.50, "aliq_icms": 0.0}
    """
    payload = {"contents": [{"parts": [{"text": prompt}, {"inlineData": {"mimeType": "application/pdf", "data": base64_data}}]}]}
    
    for attempt in range(4):
        try:
            response = requests.post(url, json=payload, timeout=120)
            if response.status_code == 200:
                data = json.loads(JSONParser.extrair_json_puro(response.json()['candidates'][0]['content']['parts'][0]['text']))
                data['file_name'] = file_name
                return data, None
            elif response.status_code == 429:
                time.sleep(15 * (attempt + 1))
                continue
            elif response.status_code in [500, 503, 504]:
                time.sleep(10 * (attempt + 1))
                continue
            else:
                return None, f"Erro API: {response.status_code}"
        except Exception as e:
            time.sleep(5)
    return None, "Desistência após falhas de IA."

# --- FUNÇÕES DO DOMÍNIO SISTEMAS ---
def limpar_cnpj(valor): return "".join(filter(str.isdigit, str(valor or "")))
def formatar_valor(valor, casas=2):
    try: return f"{float(valor):.{casas}f}".replace('.', ',')
    except: return "0,00"
def gerar_registro_0000(cnpj): return f"|0000|{limpar_cnpj(cnpj)}|"
def gerar_registro_1000(nf, obs=""):
    dt = nf.get('data', '')
    campos = ["1000", "1", limpar_cnpj(nf.get('cnpj_forn', '')), "", "1", "1102", "", str(nf.get('doc', '')), nf.get('serie', '1'), "", dt, dt, formatar_valor(nf.get('valor_total', 0)), "", obs, "C", "", "", "", "", "", "", "", "", "E"]
    return "|" + "|".join(campos) + "|" + "|" * 70
def gerar_registro_1020(nf):
    v, aliq = nf.get('valor_total', 0), nf.get('aliq_icms', 0) or 0
    return f"|1020|1||{formatar_valor(v)}|{formatar_valor(aliq)}|{formatar_valor(float(v) * (float(aliq) / 100))}|0,00|0,00|0,00|0,00|{formatar_valor(v)}||||"
def gerar_registro_1300(nf, obs=""):
    return f"|1300|{nf.get('data', '')}|55|5|{formatar_valor(nf.get('valor_total', 0))}|1|{obs}|SISTEMA|"

# --- INTERFACE VISUAL ---
st.set_page_config(page_title="Domínio Automator v7.0", layout="wide")
st.title("⚡ Domínio Automator - V7.0 (Híbrido)")

with st.sidebar:
    st.header("⚙️ Painel de Controlo")
    
    st.markdown("---")
    st.subheader("🚀 Escolha a Tecnologia:")
    metodo = st.radio("Método de Leitura:", [
        "1. MODO RELÂMPAGO (Código Offline) - RECOMENDADO", 
        "2. MODO LENTO (Inteligência Artificial Google)"
    ])
    modo_offline = "RELÂMPAGO" in metodo
    
    if modo_offline:
        st.success("O sistema vai usar Código Puro. Processa centenas de notas em poucos segundos! Não consome internet.")
    else:
        st.warning("Uso da IA do Google. Mais tolerante a layouts estranhos, mas muito mais lento.")
        api_input = st.text_area("Gemini API Keys", value=DEFAULT_KEY)
        keys_list = [k.strip() for k in api_input.replace(',', '\n').split('\n') if k.strip()]
        sel_model = st.selectbox("Versão do Gemini", ["gemini-2.0-flash", "gemini-1.5-flash"], index=0)
        delay_global = st.slider("Pausa entre notas (IA)", 2, 10, 4)
    
    st.markdown("---")
    cnpj_alvo = st.text_input("CNPJ Empresa Destino", value="33333333000191")
    texto_observacao = st.text_input("Observação (Reg. 1000/1300)", value="")

    if st.button("🔴 PARAR SISTEMA", use_container_width=True):
        st.session_state.parar = True

if 'notas_finalizadas' not in st.session_state: st.session_state.notas_finalizadas = {}
if 'falhas' not in st.session_state: st.session_state.falhas = {}
if 'parar' not in st.session_state: st.session_state.parar = False

t1, t2 = st.tabs(["🚀 Processar Fila", "💾 Gerar Importação"])

with t1:
    arquivos = st.file_uploader("Arraste todas as suas notas PDF aqui", type="pdf", accept_multiple_files=True)
    
    if arquivos:
        total_arquivos = len(arquivos)
        ja_processados = [f.name for f in arquivos if f.name in st.session_state.notas_finalizadas]
        pendentes = [f for f in arquivos if f.name not in st.session_state.notas_finalizadas]
        
        st.info(f"📊 Lote: **{total_arquivos}** | ✅ Lidas: **{len(ja_processados)}** | ⏳ Na Fila: **{len(pendentes)}**")
        
        col_btn1, col_btn2 = st.columns(2)
        btn_start = col_btn1.button("🔥 INICIAR PROCESSAMENTO GERAL", use_container_width=True)
        if col_btn2.button("🗑️ Limpar Memória", use_container_width=True):
            st.session_state.notas_finalizadas = {}
            st.session_state.falhas = {}
            st.session_state.parar = False
            st.rerun()
            
        if btn_start:
            st.session_state.parar = False
            pbar = st.progress(0)
            status_msg = st.empty()
            
            # PREPARAÇÃO DOS BYTES
            tarefas = []
            for f in pendentes:
                f.seek(0)
                tarefas.append((f.name, f.read()))
            
            # ==========================================
            # EXECUÇÃO MODO OFFLINE (SEGUNDOS)
            # ==========================================
            if modo_offline:
                status_msg.info("⚡ A ler as notas na velocidade da luz...")
                inicio = time.time()
                for f_name, f_bytes in tarefas:
                    if st.session_state.parar: break
                    
                    res, erro = extrair_dados_pdf_offline(f_name, f_bytes)
                    if res:
                        st.session_state.notas_finalizadas[f_name] = res
                        if f_name in st.session_state.falhas: del st.session_state.falhas[f_name]
                    else:
                        st.session_state.falhas[f_name] = erro
                        
                    pbar.progress(len(st.session_state.notas_finalizadas) / total_arquivos)
                
                tempo_total = round(time.time() - inicio, 2)
                status_msg.success(f"🎉 Leitura Concluída em incríveis {tempo_total} segundos!")
            
            # ==========================================
            # EXECUÇÃO MODO IA LENTO (MINUTOS)
            # ==========================================
            else:
                key_index = 0
                for idx, (f_name, f_bytes) in enumerate(tarefas):
                    if st.session_state.parar: break
                    current_key = keys_list[key_index % len(keys_list)]
                    status_msg.markdown(f"🧠 IA a ler nota **{len(st.session_state.notas_finalizadas) + 1}/{total_arquivos}**: `{f_name}`")
                    
                    res, erro = call_gemini_api_direct(f_name, f_bytes, sel_model, current_key, status_msg)
                    if res:
                        st.session_state.notas_finalizadas[f_name] = res
                        if f_name in st.session_state.falhas: del st.session_state.falhas[f_name]
                    else:
                        st.session_state.falhas[f_name] = erro
                        if "429" in str(erro) and len(keys_list) > 1: key_index += 1
                        
                    pbar.progress(len(st.session_state.notas_finalizadas) / total_arquivos)
                    status_msg.markdown(f"⏱️ Pausa de {delay_global}s...")
                    time.sleep(delay_global)
                if not st.session_state.parar: status_msg.success("🎉 Leitura IA Concluída!")
            
            st.rerun()

    if st.session_state.falhas:
        with st.expander(f"⚠️ Notas com Falha ({len(st.session_state.falhas)}) - Tente usar a IA para estas!"):
            st.table(pd.DataFrame([{"Arquivo": k, "Motivo": v} for k, v in st.session_state.falhas.items()]))

    if st.session_state.notas_finalizadas:
        st.subheader("✅ Notas Lidas")
        df_ok = pd.DataFrame(list(st.session_state.notas_finalizadas.values()))
        st.dataframe(df_ok[['doc', 'cnpj_forn', 'valor_total', 'data', 'file_name']], use_container_width=True)

with t2:
    if st.session_state.notas_finalizadas:
        st.subheader("Exportar para Sistema Domínio")
        buffer = [gerar_registro_0000(cnpj_alvo)]
        for nf in st.session_state.notas_finalizadas.values():
            buffer.append(gerar_registro_1000(nf, texto_observacao))
            buffer.append(gerar_registro_1020(nf))
            buffer.append(gerar_registro_1300(nf, texto_observacao))
        txt_final = "\r\n".join(buffer)
        st.download_button(
            label=f"📥 Transferir Ficheiro de Importação ({len(st.session_state.notas_finalizadas)} Notas)",
            data=txt_final.encode('latin-1', errors='replace'),
            file_name=f"lote_dominio_FAST_{datetime.now().strftime('%H%M')}.txt",
            mime="text/plain",
            use_container_width=True
        )

st.divider()
st.caption("v7.0 - Integração de Motor de Extração Offline de Altíssima Velocidade.")
