import streamlit as st
import pandas as pd
from datetime import datetime
import base64
import json
import time
import requests
import re

class JSONParser:
    @staticmethod
    def extrair_json_puro(texto):
        """Extrai apenas o conteúdo JSON, ignorando conversas e formatações da IA."""
        try:
            match = re.search(r'\{.*\}', texto, re.DOTALL)
            if match:
                return match.group(0)
            return texto
        except:
            return texto

# --- CONFIGURAÇÃO DA API GEMINI ---
DEFAULT_KEY = ""

def call_gemini_api_direct(file_name, file_bytes, model_name, api_key, status_placeholder):
    """
    Chamada direta à API via requests.
    Feita para ser 100% à prova de erros de payload (400) e lidar bem com rate limit (429) e sobrecarga (503).
    """
    if not api_key:
        return None, "Chave de API ausente."

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key.strip()}"
    base64_data = base64.b64encode(file_bytes).decode('utf-8')
    
    prompt = """
    Extraia os dados desta Nota Fiscal de Serviço (NFS-e).
    Responda APENAS com um objeto JSON válido, sem a marcação ```json e sem texto adicional.
    
    Campos exatos:
    {
      "doc": 162,
      "serie": "1",
      "data": "01/03/2026",
      "cnpj_forn": "14243715000180",
      "valor_total": 16018.50,
      "aliq_icms": 0.0
    }
    """

    payload = {
        "contents": [{
            "parts": [
                {"text": prompt},
                {"inlineData": {"mimeType": "application/pdf", "data": base64_data}}
            ]
        }]
    }

    # 5 tentativas para dar mais tolerância aos servidores cheios (503)
    for attempt in range(5):
        try:
            response = requests.post(url, json=payload, timeout=120)
            
            if response.status_code == 200:
                res_json = response.json()
                raw_text = res_json['candidates'][0]['content']['parts'][0]['text']
                
                json_str = JSONParser.extrair_json_puro(raw_text)
                data = json.loads(json_str)
                data['file_name'] = file_name
                return data, None
                
            elif response.status_code == 429: # Erro de Cota / Limite
                wait_time = 15 * (attempt + 1)
                status_placeholder.warning(f"⏳ Limite da cota (429). Pausando por {wait_time}s...")
                time.sleep(wait_time)
                continue
                
            elif response.status_code in [500, 503, 504]: # Servidores sobrecarregados
                wait_time = 10 * (attempt + 1)
                status_placeholder.warning(f"⏳ Servidores do Google sobrecarregados (Erro {response.status_code}). Nova tentativa em {wait_time}s...")
                time.sleep(wait_time)
                continue
                
            elif response.status_code == 404:
                return None, f"Erro 404: O modelo '{model_name}' não existe nesta chave."
                
            elif response.status_code == 400:
                err_msg = response.json().get('error', {}).get('message', 'Erro desconhecido')
                return None, f"Erro 400 (Chave/Formato): {err_msg}"
                
            else:
                return None, f"Erro inesperado do Google: {response.status_code} - {response.text[:100]}"
                
        except json.JSONDecodeError:
             return None, "A IA não conseguiu formatar os dados corretamente."
        except Exception as e:
            if attempt == 4:
                return None, f"Falha de conexão: {str(e)}"
            time.sleep(5)
            
    return None, "Desistência após 5 bloqueios/sobrecargas consecutivas do Google."

# --- FUNÇÕES DO DOMÍNIO SISTEMAS ---
def limpar_cnpj(valor):
    return "".join(filter(str.isdigit, str(valor or "")))

def formatar_valor(valor, casas=2):
    try:
        return f"{float(valor):.{casas}f}".replace('.', ',')
    except:
        return "0,00"

def gerar_registro_0000(cnpj):
    return f"|0000|{limpar_cnpj(cnpj)}|"

def gerar_registro_1000(nf, obs=""):
    dt = nf.get('data', '')
    # O campo obs preenche a posição de observação/histórico do registro 1000
    campos = ["1000", "1", limpar_cnpj(nf.get('cnpj_forn', '')), "", "1", "1102", "", str(nf.get('doc', '')), nf.get('serie', '1'), "", dt, dt, formatar_valor(nf.get('valor_total', 0)), "", obs, "C", "", "", "", "", "", "", "", "", "E"]
    return "|" + "|".join(campos) + "|" + "|" * 70

def gerar_registro_1020(nf):
    v = nf.get('valor_total', 0)
    aliq = nf.get('aliq_icms', 0) or 0
    val_icms = float(v) * (float(aliq) / 100)
    return f"|1020|1||{formatar_valor(v)}|{formatar_valor(aliq)}|{formatar_valor(val_icms)}|0,00|0,00|0,00|0,00|{formatar_valor(v)}||||"

def gerar_registro_1300(nf, obs=""):
    # O campo obs preenche o complemento do registro 1300
    return f"|1300|{nf.get('data', '')}|55|5|{formatar_valor(nf.get('valor_total', 0))}|1|{obs}|SISTEMA|"

# --- INTERFACE VISUAL ---
st.set_page_config(page_title="Domínio Automator v5.2", layout="wide")

st.title("⚡ Domínio Automator - Fila Resiliente (V5.2)")

with st.sidebar:
    st.header("⚙️ Painel de Controlo")
    
    api_input = st.text_area("Gemini API Keys", value=DEFAULT_KEY, help="Se usar mais de uma, separe por linhas.")
    keys_list = [k.strip() for k in api_input.replace(',', '\n').split('\n') if k.strip()]
    
    lista_modelos = ["gemini-2.5-flash", "gemini-2.0-flash", "gemini-1.5-flash"]
    sel_model = st.selectbox("Versão do Gemini", lista_modelos, index=0)
    
    cnpj_alvo = st.text_input("CNPJ Empresa Destino", value="33333333000191")
    
    st.markdown("---")
    st.subheader("📝 Textos Livres")
    # Campo adicionado para o utilizador escolher o texto da observação ou deixar vazio
    texto_observacao = st.text_input("Observação/Histórico (Reg. 1000 e 1300)", value="", help="Deixe vazio para não sair nenhum texto, ou escreva algo como 'NFSE'.")
    
    st.markdown("---")
    st.subheader("⏱️ Velocidade")
    delay_global = st.slider("Pausa Obrigatória (Segundos)", 2, 10, 4)
    st.caption("Pausa de 4s recomendada para o limite gratuito.")

    if st.button("🔴 PARAR SISTEMA", use_container_width=True):
        st.session_state.parar = True

# Inicialização da memória do Streamlit
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
        
        st.info(f"📊 Total: **{total_arquivos}** | ✅ Concluídas: **{len(ja_processados)}** | ⏳ Na Fila: **{len(pendentes)}**")
        
        col_btn1, col_btn2 = st.columns(2)
        iniciar = col_btn1.button("🔥 Iniciar/Retomar Fila", use_container_width=True)
        
        if col_btn2.button("🗑️ Limpar Memória", use_container_width=True):
            st.session_state.notas_finalizadas = {}
            st.session_state.falhas = {}
            st.session_state.parar = False
            st.rerun()
            
        if iniciar:
            st.session_state.parar = False
            if not keys_list:
                st.error("Insira pelo menos uma Chave de API.")
            else:
                pbar = st.progress(len(ja_processados) / total_arquivos if total_arquivos > 0 else 0)
                status_msg = st.empty()
                
                key_index = 0
                
                # LAÇO SEQUENCIAL
                for idx, f in enumerate(pendentes):
                    if st.session_state.parar:
                        status_msg.warning("Processamento interrompido. Pode retomar quando quiser.")
                        break
                        
                    current_key = keys_list[key_index % len(keys_list)]
                    status_msg.markdown(f"📄 A ler nota **{len(st.session_state.notas_finalizadas) + 1} de {total_arquivos}**: `{f.name}`")
                    
                    # Leitura segura do arquivo
                    f.seek(0)
                    file_bytes = f.read()
                    
                    if not file_bytes:
                        st.session_state.falhas[f.name] = "Ficheiro corrompido ou ilegível."
                        continue
                    
                    # Faz a chamada à API
                    res, erro = call_gemini_api_direct(f.name, file_bytes, sel_model, current_key, status_msg)
                    
                    if res:
                        st.session_state.notas_finalizadas[f.name] = res
                        if f.name in st.session_state.falhas: 
                            del st.session_state.falhas[f.name]
                    else:
                        st.session_state.falhas[f.name] = erro
                        if "429" in str(erro) or "Limite" in str(erro) or "503" in str(erro):
                            if len(keys_list) > 1:
                                key_index += 1
                                status_msg.info("A tentar usar a próxima Chave de API...")
                    
                    total_ok = len(st.session_state.notas_finalizadas)
                    pbar.progress(total_ok / total_arquivos)
                    
                    status_msg.markdown(f"⏱️ Pausa de segurança ({delay_global}s) para evitar bloqueio do Google...")
                    time.sleep(delay_global)
                
                if not st.session_state.parar:
                    status_msg.success("🎉 Leitura de todo o lote concluída!")
                st.rerun()

    if st.session_state.falhas:
        with st.expander(f"⚠️ Notas com Falha ({len(st.session_state.falhas)}) - Leia a coluna 'Motivo'"):
            st.table(pd.DataFrame([{"Arquivo": k, "Motivo": v} for k, v in st.session_state.falhas.items()]))

    if st.session_state.notas_finalizadas:
        st.subheader("✅ Notas Lidas com Sucesso")
        df_ok = pd.DataFrame(list(st.session_state.notas_finalizadas.values()))
        st.dataframe(df_ok[['doc', 'cnpj_forn', 'valor_total', 'data', 'file_name']], use_container_width=True)

with t2:
    if st.session_state.notas_finalizadas:
        st.subheader("Exportar para Sistema Domínio")
        buffer = [gerar_registro_0000(cnpj_alvo)]
        
        # Passa o texto configurado pelo utilizador para os geradores
        for nf in st.session_state.notas_finalizadas.values():
            buffer.append(gerar_registro_1000(nf, texto_observacao))
            buffer.append(gerar_registro_1020(nf))
            buffer.append(gerar_registro_1300(nf, texto_observacao))
            
        txt_final = "\r\n".join(buffer)
        
        st.download_button(
            label=f"📥 Transferir Ficheiro de Importação ({len(st.session_state.notas_finalizadas)} Notas)",
            data=txt_final.encode('latin-1', errors='replace'),
            file_name=f"lote_dominio_V5_{datetime.now().strftime('%H%M')}.txt",
            mime="text/plain",
            use_container_width=True
        )
    else:
        st.info("Nenhuma nota processada. Vá à primeira aba para iniciar a fila.")

st.divider()
st.caption("v5.2 - Campo de observação dinâmico adicionado.")
