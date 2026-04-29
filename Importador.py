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
# MOTOR OFFLINE - TÉCNICA HÍBRIDA (V9.4)
# ==========================================
def extrair_dados_pdf_offline(file_name, file_bytes):
    """
    Lê o PDF com precisão máxima. Usa Texto Denso para CNPJs e Datas (rígido), 
    Texto Limpo para Valores, e o Nome do Ficheiro como último recurso (Truque do Nome).
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
        # 1. NÚMERO DO DOCUMENTO (Busca de Trechos + Jargões Fatura/Recibo/NF + Nome Arquivo)
        # ---------------------------------------------------------
        doc_str = None
        
        # 1.1 Procura direta por jargões curtos
        padroes_diretos = [
            r"\bNF\s*[N0Oº°]?\s*[:.-]?\s*0*(\d+)\b",  
            r"\bDANFE\s*[N0Oº°]?\s*[:.-]?\s*0*(\d+)\b", 
            r"\bN[Oº°]\s*[:.-]?\s*0*(\d+)\b",
            r"\bFATURA\s*[N0Oº°]?\s*[:.-]?\s*0*(\d+)\b",
            r"\bRECIBO\s*[N0Oº°]?\s*[:.-]?\s*0*(\d+)\b"
        ]
        for padrao in padroes_diretos:
            match = re.search(padrao, texto_limpo)
            if match and match.group(1) not in ['2024', '2025', '2026', '2027']:
                doc_str = match.group(1)
                break

        # 1.2 Procura o rótulo e extrai uma amostra de 80 letras a seguir.
        if not doc_str:
            padroes_doc = [
                r"NUMERO DA NFS-E(.{0,80})",
                r"NUMERO DA NOTA(.{0,80})",
                r"NOTA FISCAL ELETRONICA(.{0,80})",
                r"NOTA FISCAL(.{0,80})",
                r"NOTA:(.{0,80})",
                r"NFS-E(.{0,80})",
                r"NUMERO(.{0,80})"
            ]
            
            for padrao in padroes_doc:
                match = re.search(padrao, texto_limpo)
                if match:
                    trecho = match.group(1)
                    numeros = re.findall(r"(?<!/)\b(\d+)\b(?!/)", trecho)
                    numeros_validos = [n for n in numeros if n not in ['2024', '2025', '2026', '2027'] and len(n) <= 15]
                    if numeros_validos:
                        doc_str = numeros_validos[0]
                        break
                    
        # 1.3 Fallback GINFES/Guarulhos
        if not doc_str:
            ginfes_match = re.search(r"(?<!/)\b(\d+)\b\s+(?![A-Z]{9}\b)(?!\d{9}\b)[A-Z0-9]{9}\b", texto_limpo)
            if ginfes_match and ginfes_match.group(1) not in ['2024', '2025', '2026', '2027']:
                doc_str = ginfes_match.group(1)
                
        # 1.4 Fallback Final no Texto Denso
        if not doc_str:
            for padrao in [
                r"NUMERODANFS-E[^\d]{0,20}0*(\d+)(?!\/)",
                r"NUMERODANOTA[^\d]{0,20}0*(\d+)(?!\/)"
            ]:
                matches = re.findall(padrao, texto_denso)
                for m in matches:
                    m_clean = m.lstrip('0') or '0'
                    if m_clean not in ['2024', '2025', '2026', '2027'] and len(m_clean) < 15:
                        doc_str = m_clean
                        break
                if doc_str: break

        # 1.5 Fallback pelo NOME DO ARQUIVO (O Truque do Nome)
        if not doc_str and file_name:
            # Ex: NF_33, NF 92, NOTA_10
            match_nome = re.search(r'\b(?:NF|NOTA|FATURA)[_ -]*[NO]?[_ -]*0*(\d+)\b', file_name.upper())
            if match_nome and match_nome.group(1) not in ['2024', '2025', '2026', '2027']:
                doc_str = match_nome.group(1)

        if doc_str:
            dados["doc"] = int(doc_str)

        # ---------------------------------------------------------
        # 2. DATA DE COMPETÊNCIA / EMISSÃO (Texto Denso)
        # ---------------------------------------------------------
        padroes_data = [
            r"COMPETENCIADANFS-E[^\d]*(\d{2}/\d{2}/\d{4})",
            r"EMISSAO[^\d]*(\d{2}/\d{2}/\d{4})",
            r"DATA[^\d]*(\d{2}/\d{2}/\d{4})",
            r"(\d{2}/\d{2}/\d{4})"
        ]
        for padrao in padroes_data:
            match = re.search(padrao, texto_denso)
            if match:
                dados["data"] = match.group(1)
                break

        # ---------------------------------------------------------
        # 3. CNPJ Emitente (Texto Denso - Infalível)
        # ---------------------------------------------------------
        cnpjs = re.findall(r"(\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2})", texto_denso)
        if not cnpjs:
            cnpj_match = re.search(r"CNPJ[^\d]*(\d{14})", texto_denso)
            if cnpj_match: cnpjs = [cnpj_match.group(1)]
        if cnpjs:
            dados["cnpj_forn"] = re.sub(r"\D", "", cnpjs[0])

        # ---------------------------------------------------------
        # 4. VALOR TOTAL (Texto Limpo + Formatos US/BR + Inteiros)
        # ---------------------------------------------------------
        regex_dinheiro = r"\b(\d{1,10}(?:[.,]\d{3})*[.,]\d{2})\b"
        
        padroes_valor = [
            rf"ALIQUOTA.{{0,80}}?{regex_dinheiro}", 
            rf"VALOR LIQUIDO.{{0,80}}?{regex_dinheiro}",
            rf"VALOR DOS SERVICOS.{{0,80}}?{regex_dinheiro}",
            rf"VALOR TOTAL.{{0,80}}?{regex_dinheiro}",
            rf"VALOR DA NOTA.{{0,80}}?{regex_dinheiro}",
            rf"TOTAL.{{0,80}}?{regex_dinheiro}",
            rf"R\$.{{0,80}}?{regex_dinheiro}",
            rf"VALOR.{{0,80}}?{regex_dinheiro}"
        ]
        
        v_raw = None
        for padrao in padroes_valor:
            matches = re.findall(padrao, texto_limpo)
            for val in matches:
                val_check = val.replace(',', '.')
                if val_check not in ["0.00", "0.01", "00.00"]: 
                    v_raw = val
                    break
            if v_raw: break

        # Conversão Matemática Universal
        if v_raw:
            if len(v_raw) > 3 and v_raw[-3] in [',', '.']:
                v_str = re.sub(r'[.,]', '', v_raw[:-3]) + '.' + v_raw[-2:]
            else:
                v_str = re.sub(r'[.,]', '', v_raw)
            dados["valor_total"] = float(v_str)
        else:
            # FALLBACK SUPREMO MATEMÁTICO (Agora imune aos ZEROS)
            todos_valores = re.findall(regex_dinheiro, texto_limpo)
            valores_float = []
            for val in todos_valores:
                if len(val) > 3 and val[-3] in [',', '.']:
                    v_str = re.sub(r'[.,]', '', val[:-3]) + '.' + val[-2:]
                else:
                    v_str = re.sub(r'[.,]', '', val)
                    
                v_float = float(v_str)
                if v_float > 0: # CRUCIAL: Ignora "0,00" para não dar R$: 0.0
                    valores_float.append(v_float)
            
            if valores_float:
                dados["valor_total"] = max(valores_float)
                
            # Fallback Inteiros e Nome do Arquivo (Ex: R$ 51000 ou ..._R_51000.pdf)
            if not dados.get("valor_total"):
                match_inteiro = re.search(r'R\$\s*(\d{2,10}(?:[.,]\d{3})*)(?!\d)', texto_limpo)
                if match_inteiro:
                    dados["valor_total"] = float(re.sub(r'[.,]', '', match_inteiro.group(1)))
                elif file_name:
                    match_val_nome = re.search(r'_R\$?[_ -]*(\d{2,10})\b', file_name.upper())
                    if match_val_nome:
                        dados["valor_total"] = float(match_val_nome.group(1))
            
        # ---------------------------------------------------------
        # VALIDAÇÃO FINAL
        # ---------------------------------------------------------
        if dados["doc"] is not None and dados["cnpj_forn"] and dados["valor_total"] is not None:
            return dados, None
        else:
            return None, f"Leitura Incompleta -> Doc:{dados.get('doc')} | CNPJ:{dados.get('cnpj_forn')} | R$:{dados.get('valor_total')}"
            
    except Exception as e:
        return None, f"Erro no Motor Offline: {str(e)}"

# ==========================================
# MOTOR IA (GEMINI) - FALLBACK COM DETETIVE DE ERROS
# ==========================================
DEFAULT_KEY = "" 

def call_gemini_api_direct(file_name, file_bytes, model_name, api_key, status_placeholder):
    if not api_key: return None, "Chave de API ausente. Insira uma nova chave válida."
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
                if "API Key" in err_msg or "API key" in err_msg:
                    return None, "🚨 ERRO CRÍTICO: A sua Chave de API foi bloqueada ou é inválida. Por favor, gere uma nova no Google AI Studio."
                return None, f"Erro 400 do Google: {err_msg}"
            elif response.status_code == 404:
                return None, f"Erro HTTP 404: Modelo '{model_name}' não encontrado. Tente a opção '-latest' na barra lateral."
            elif response.status_code == 429:
                wait_time = 20 * (attempt + 1)
                last_err = f"Limite de requisições excedido (Erro 429). Aguardando {wait_time}s..."
                status_placeholder.warning(f"⏳ Cota atingida! Em pausa obrigatória por {wait_time}s...")
                time.sleep(wait_time)
                continue
            elif response.status_code in [500, 503, 504]:
                wait_time = 15 * (attempt + 1)
                last_err = f"Servidores da Google sobrecarregados (Erro {response.status_code}). Aguardando {wait_time}s..."
                status_placeholder.warning(f"⏳ Servidores lentos! Em pausa por {wait_time}s...")
                time.sleep(wait_time)
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
            
    return None, f"Falha na IA. Último erro: {last_err} (Dica: Se esgotou as 4 tentativas, a sua Chave atingiu o limite DIÁRIO de 1500 notas. Tente novamente amanhã)."

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
st.set_page_config(page_title="Domínio Automator v9.4", layout="wide")
st.title("Importador de Notas Automatizado.")

with st.sidebar:
    st.header("⚙️ Painel de Controlo")
    
    st.markdown("---")
    st.subheader("🚀 Escolha a Tecnologia:")
    metodo = st.radio("Método de Leitura:", [
        "1. MODO RÁPIDO (Código Offline) - RECOMENDADO", 
        "2. MODO LENTO (Inteligência Artificial Google)"
    ])
    modo_offline = "RÁPIDO" in metodo
    
    if modo_offline:
        st.success("Técnica Híbrida v9.4 ativada. Lógica de Arquivos e Prevenção de Zeros Ativos.")
    else:
        st.warning("Uso da IA (Gemini). Cole uma chave válida abaixo se quiser usar a IA.")
        api_input = st.text_input("Nova Gemini API Key", value=DEFAULT_KEY, type="password")
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
            
            tarefas = []
            for f in pendentes:
                f.seek(0)
                tarefas.append((f.name, f.read()))
            
            total_tarefas = len(tarefas)
            
            if total_tarefas == 0:
                status_msg.warning("⚠️ Todas as notas selecionadas já foram processadas ou a lista está vazia!")
            else:
                if modo_offline:
                    status_msg.info("⚡ A ler as notas através de código puro...")
                    inicio = time.time()
                    for idx, (f_name, f_bytes) in enumerate(tarefas):
                        if st.session_state.parar: break
                        
                        res, erro = extrair_dados_pdf_offline(f_name, f_bytes)
                        if res:
                            st.session_state.notas_finalizadas[f_name] = res
                            if f_name in st.session_state.falhas: del st.session_state.falhas[f_name]
                        else:
                            st.session_state.falhas[f_name] = erro
                            
                        pbar.progress(min((idx + 1) / total_tarefas, 1.0))
                    
                    tempo_total = round(time.time() - inicio, 2)
                    if not st.session_state.parar:
                        status_msg.success(f"🎉 Leitura Concluída em {tempo_total} segundos!")
                
                else:
                    if not keys_list:
                        status_msg.error("Cole uma nova Chave de API na barra lateral para usar a IA.")
                    else:
                        key_index = 0
                        for idx, (f_name, f_bytes) in enumerate(tarefas):
                            if st.session_state.parar: break
                            current_key = keys_list[key_index % len(keys_list)]
                            status_msg.markdown(f"🧠 IA a ler nota **{idx + 1}/{total_tarefas}**: `{f_name}`")
                            
                            res, erro = call_gemini_api_direct(f_name, f_bytes, sel_model, current_key, status_msg)
                            if res:
                                st.session_state.notas_finalizadas[f_name] = res
                                if f_name in st.session_state.falhas: del st.session_state.falhas[f_name]
                            else:
                                st.session_state.falhas[f_name] = erro
                                if "429" in str(erro) and len(keys_list) > 1: key_index += 1
                                if "ERRO CRÍTICO" in str(erro): 
                                    break 
                                
                            pbar.progress(min((idx + 1) / total_tarefas, 1.0))
                            status_msg.markdown(f"⏱️ Pausa de {delay_global}s...")
                            time.sleep(delay_global)
                        if not st.session_state.parar:
                            status_msg.success("🎉 Leitura IA Concluída!")

    if st.session_state.falhas:
        with st.expander(f"⚠️ Notas com Falha ({len(st.session_state.falhas)}) - Verifique o motivo!", expanded=True):
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
            file_name=f"lote_dominio_V9.4_{datetime.now().strftime('%H%M')}.txt",
            mime="text/plain",
            use_container_width=True
        )

st.divider()
st.caption("by Albert - Vocabulário Nacional V9.4.")
