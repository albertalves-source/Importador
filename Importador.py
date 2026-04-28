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
# MOTOR OFFLINE - TÉCNICA DE "TEXTO DENSO"
# ==========================================
def extrair_dados_pdf_offline(file_name, file_bytes):
    """
    Lê o PDF de forma indestrutível, removendo todos os espaços e acentos para 
    evitar problemas de formatação da biblioteca PyPDF2.
    """
    try:
        leitor = PyPDF2.PdfReader(io.BytesIO(file_bytes))
        texto_bruto = ""
        for pagina in leitor.pages:
            if pagina.extract_text():
                texto_bruto += pagina.extract_text() + " "
                
        # Transforma o texto numa massa densa sem espaços e em maiúsculas
        texto_denso = re.sub(r'\s+', '', texto_bruto).upper()
        
        # Remove acentos (Ex: NÚMERO -> NUMERO, SERVIÇO -> SERVICO)
        texto_denso = ''.join(c for c in unicodedata.normalize('NFD', texto_denso) if unicodedata.category(c) != 'Mn')
        
        dados = {"doc": None, "serie": "1", "data": None, "cnpj_forn": None, "valor_total": None, "aliq_icms": 0.0, "file_name": file_name}
        
        # 1. NÚMERO DO DOCUMENTO (Super flexível ignorando lixo entre a palavra e o número)
        doc_match = re.search(r"NUMERODANFS-E[^\d]*0*(\d+)", texto_denso)
        if not doc_match: doc_match = re.search(r"NUMERODANOTA[^\d]*0*(\d+)", texto_denso)
        if not doc_match: doc_match = re.search(r"NFS-E[^\d]*0*(\d+)", texto_denso)
        if doc_match: dados["doc"] = int(doc_match.group(1))
            
        # 2. DATA
        data_match = re.search(r"COMPETENCIADANFS-E[^\d]*(\d{2}/\d{2}/\d{4})", texto_denso)
        if not data_match: data_match = re.search(r"EMISSAO[^\d]*(\d{2}/\d{2}/\d{4})", texto_denso)
        if not data_match: data_match = re.search(r"(\d{2}/\d{2}/\d{4})", texto_denso) # Pega a primeira data que aparecer
        if data_match: dados["data"] = data_match.group(1)
            
        # 3. CNPJ (O primeiro que aparece é sempre o do Emitente)
        cnpjs = re.findall(r"(\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2})", texto_denso)
        if cnpjs: dados["cnpj_forn"] = re.sub(r"\D", "", cnpjs[0])
            
        # 4. VALOR TOTAL (Super flexível para não depender de R$ exatos)
        valor_match = re.search(r"VALORDOSERVICO[^\d]*([\d\.,]+)", texto_denso)
        if not valor_match: valor_match = re.search(r"VALORTOTALDOSERVICO[^\d]*([\d\.,]+)", texto_denso)
        if not valor_match: valor_match = re.search(r"VALORLIQUIDODANFS-E[^\d]*([\d\.,]+)", texto_denso)
        if not valor_match: valor_match = re.search(r"VALORTOTAL[^\d]*([\d\.,]+)", texto_denso)
        if not valor_match: valor_match = re.search(r"R\$[^\d]*([\d\.,]+)", texto_denso) # Fallback
        
        if valor_match:
            v_raw = valor_match.group(1)
            # Lógica segura para converter a moeda brasileira para decimal (Python)
            if ',' in v_raw:
                v_str = v_raw.replace('.', '').replace(',', '.')
            else:
                if v_raw.count('.') >= 1:
                    partes = v_raw.rsplit('.', 1)
                    if len(partes[1]) == 2:
                        v_str = partes[0].replace('.', '') + '.' + partes[1]
                    else:
                        v_str = v_raw.replace('.', '')
                else:
                    v_str = v_raw
            dados["valor_total"] = float(v_str)
            
        # Validação final
        if dados["doc"] and dados["cnpj_forn"] and dados["valor_total"] is not None:
            return dados, None
        else:
            return None, f"Leitura Incompleta -> Doc:{dados['doc']} | CNPJ:{dados['cnpj_forn']} | R$:{dados['valor_total']}"
            
    except Exception as e:
        return None, f"Erro no Motor Offline: {str(e)}"

# ==========================================
# MOTOR IA (GEMINI) - FALLBACK COM DETETIVE DE ERROS
# ==========================================
DEFAULT_KEY = "AIzaSyB_mDR97ABexRXVSUQkxd_bgYjL_xHKaw8"

def call_gemini_api_direct(file_name, file_bytes, model_name, api_key, status_placeholder):
    if not api_key: return None, "Chave de API ausente."
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key.strip()}"
    base64_data = base64.b64encode(file_bytes).decode('utf-8')
    prompt = """Extraia os dados desta Nota Fiscal de Serviço. Responda APENAS com um objeto JSON válido.
    Campos exatos: {"doc": 162, "serie": "1", "data": "01/03/2026", "cnpj_forn": "14243715000180", "valor_total": 16018.50, "aliq_icms": 0.0}"""
    payload = {"contents": [{"parts": [{"text": prompt}, {"inlineData": {"mimeType": "application/pdf", "data": base64_data}}]}]}
    
    last_err = "Nenhum erro reportado."
    for attempt in range(4):
        try:
            response = requests.post(url, json=payload, timeout=120)
            if response.status_code == 200:
                resp_json = response.json()
                
                # Validação rigorosa da resposta da IA
                if 'candidates' not in resp_json or not resp_json['candidates']:
                    last_err = "IA não enviou candidatos na resposta (possível bloqueio do PDF)."
                    time.sleep(5)
                    continue
                    
                parts = resp_json['candidates'][0].get('content', {}).get('parts', [])
                if not parts:
                    last_err = "Resposta da IA sem texto."
                    time.sleep(5)
                    continue
                    
                raw_text = parts[0].get('text', '')
                json_str = JSONParser.extrair_json_puro(raw_text)
                
                if not json_str:
                    last_err = "JSON não encontrado no texto da IA."
                    time.sleep(5)
                    continue
                    
                data = json.loads(json_str)
                data['file_name'] = file_name
                return data, None
                
            elif response.status_code == 400:
                err_msg = response.json().get('error', {}).get('message', 'Formato Rejeitado')
                return None, f"Erro 400 do Google: {err_msg}"
            elif response.status_code == 404:
                return None, f"Erro HTTP 404: Modelo '{model_name}' não encontrado. Tente a opção '-latest' na barra lateral."
            elif response.status_code == 429:
                time.sleep(15 * (attempt + 1))
                continue
            elif response.status_code in [500, 503, 504]:
                time.sleep(10 * (attempt + 1))
                continue
            else:
                last_err = f"Erro HTTP {response.status_code}"
                time.sleep(5)
        except json.JSONDecodeError as e:
            last_err = f"Falha ao interpretar JSON: {str(e)}"
            time.sleep(5)
        except Exception as e:
            last_err = f"Exceção técnica: {str(e)}"
            time.sleep(5)
            
    return None, f"Desistência após falhas da IA. Último erro interno: {last_err}"

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
st.set_page_config(page_title="Domínio Automator v8.2", layout="wide")
st.title("⚡ Domínio Automator - V8.2 (Motor Extra Forte)")

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
        st.success("Técnica de Texto Denso ativada. Processamento em Segundos!")
    else:
        st.warning("Uso da IA (Gemini 2.0). Processamento mais lento, mas altamente preciso.")
        api_input = st.text_area("Gemini API Keys", value=DEFAULT_KEY)
        keys_list = [k.strip() for k in api_input.replace(',', '\n').split('\n') if k.strip()]
        
        # Atualizado para usar Gemini 2.0 Flash como padrão absoluto
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
            
            tarefas = []
            for f in pendentes:
                f.seek(0)
                tarefas.append((f.name, f.read()))
            
            if modo_offline:
                status_msg.info("⚡ A ler as notas através de código puro...")
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
                status_msg.success(f"🎉 Leitura Concluída em {tempo_total} segundos!")
            
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
            file_name=f"lote_dominio_V8.2_{datetime.now().strftime('%H%M')}.txt",
            mime="text/plain",
            use_container_width=True
        )

st.divider()
st.caption("v8.2 - Algoritmos de busca extra-flexíveis (Expressões Regulares) para o Modo Relâmpago.")
