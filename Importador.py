import streamlit as st
import pandas as pd
from datetime import datetime
import base64
import json
import io
import PyPDF2
import unicodedata
import re
import requests

# --- UTILITÁRIOS ---
class JSONParser:
    @staticmethod
    def extrair_json_puro(texto):
        try:
            match = re.search(r'\{.*\}', texto, re.DOTALL)
            return match.group(0) if match else texto
        except: return texto

def limpar_cnpj(v): return "".join(filter(str.isdigit, str(v or "")))
def formatar_valor(v): return f"{float(v):.2f}".replace('.', ',')

def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Confronto')
    return output.getvalue()

# Inteligência para ler o relatório bruto exportado do sistema Domínio
def carregar_planilha_segura(arquivo):
    arquivo.seek(0)
    if arquivo.name.lower().endswith('.csv'):
        try:
            df_temp = pd.read_csv(arquivo, header=None, dtype=str, sep=None, engine='python')
        except:
            arquivo.seek(0)
            df_temp = pd.read_csv(arquivo, header=None, dtype=str)
    else:
        df_temp = pd.read_excel(arquivo, header=None, dtype=str)
        
    # Caça a linha onde o cabeçalho real começa
    idx_header = 0
    for i, row in df_temp.iterrows():
        valores = [str(x).strip().upper() for x in row.values if pd.notna(x)]
        if "AC." in valores or "ACUMULADOR" in valores or "CNPJ" in valores or "CNPJ_FORN" in valores or "FORNECEDOR" in valores:
            idx_header = i
            break
            
    df = df_temp.iloc[idx_header+1:].copy()
    colunas = [str(c).strip().upper() for c in df_temp.iloc[idx_header].values]
    
    # Limpa nomes bizarros (UNNAMED, NAN, etc)
    colunas_limpas = []
    for i, c in enumerate(colunas):
        if c in ['NAN', 'NONE', '']: colunas_limpas.append(f"COL_{i}")
        else: colunas_limpas.append(c)
    df.columns = colunas_limpas
    
    # Padronização do layout do Domínio
    if 'AC.' in df.columns: df.rename(columns={'AC.': 'ACUMULADOR'}, inplace=True)
    
    # Se o Domínio não gerou coluna CNPJ, extraímos do nome do Fornecedor (ex: "64.717.612 PAULO VINICIUS")
    if 'CNPJ' not in df.columns and 'FORNECEDOR' in df.columns:
        def extrair_cnpj(texto):
            nums = limpar_cnpj(str(texto))
            return nums if len(nums) >= 11 else ""
        df['CNPJ'] = df['FORNECEDOR'].apply(extrair_cnpj)
        
    # Mantém a compatibilidade com as colunas geradas pelo nosso Módulo 1
    col_map = {
        'DOC': 'doc', 
        'CNPJ_FORN': 'cnpj_forn', 
        'VALOR_TOTAL': 'valor_total', 
        'DATA': 'data',
        'FILE_NAME': 'file_name'
    }
    df.rename(columns=col_map, inplace=True)
    
    return df

# --- MOTORES DE LEITURA (PDF / IA) ---
def extrair_dados_pdf_offline(file_name, file_bytes, cnpj_destino_usuario):
    try:
        leitor = PyPDF2.PdfReader(io.BytesIO(file_bytes))
        texto_bruto = " ".join([p.extract_text() or "" for p in leitor.pages])
        if len(texto_bruto.strip()) < 50: return None, "PDF é imagem (Scan). Use MODO IA."
        
        texto_limpo = re.sub(r'\s+', ' ', texto_bruto).upper()
        texto_limpo = ''.join(c for c in unicodedata.normalize('NFD', texto_limpo) if unicodedata.category(c) != 'Mn')
        texto_denso = texto_limpo.replace(' ', '')
        
        dados = {"doc": None, "serie": "1", "data": None, "cnpj_forn": None, "valor_total": None, "acumulador": "1", "file_name": file_name}
        
        # Busca CNPJ Fornecedor
        cnpj_alvo = limpar_cnpj(cnpj_destino_usuario)
        docs = list(dict.fromkeys(re.findall(r'\d{14}|\d{11}', texto_denso)))
        for d in docs:
            if d != cnpj_alvo and d != "00000000000000" and not d.startswith("25155"):
                dados["cnpj_forn"] = d
                break
        if not dados["cnpj_forn"]: dados["cnpj_forn"] = docs[0] if docs else "00000000000000"

        # Busca Número Documento
        doc_str = None
        for p in [r"NFS-E[^\d]{0,30}?0*(\d+)", r"NUMERO[^\d]{0,50}?0*(\d+)", r"NF-?E?\s*[:.-]?\s*0*(\d+)"]:
            m = re.findall(p, texto_limpo)
            validos = [x for x in m if x not in ['2024','2025','2026','0']]
            if validos: {doc_str := validos[0]}; break
        
        if not doc_str: # Fallback nome arquivo
            nums = re.findall(r'\d+', file_name)
            if nums: doc_str = max(nums, key=len)

        dados["doc"] = int(doc_str) if doc_str else 1
        
        # Data
        dt = re.search(r"(\d{2}/\d{2}/\d{4})", texto_denso)
        dados["data"] = dt.group(1) if dt else datetime.now().strftime("%d/%m/%Y")

        # Valor
        v_matches = re.findall(r"(\d{1,10}(?:[.,]\d{3})*[.,]\d{2})", texto_limpo)
        if v_matches:
            vals = []
            for v in v_matches:
                try:
                    vf = float(re.sub(r'[.,]', '', v[:-3]) + '.' + v[-2:])
                    if 0.5 < vf < 9999999.0: vals.append(vf)
                except: continue
            if vals: dados["valor_total"] = max(vals)

        return (dados, None) if dados["valor_total"] else (None, "Valor não encontrado.")
    except Exception as e: return None, str(e)

# --- GERAÇÃO DOMÍNIO ---
def gerar_registro_0000(cnpj): return f"|0000|{limpar_cnpj(cnpj)}|"
def gerar_registro_1000(nf, obs):
    dt, acum = nf.get('data',''), str(nf.get('acumulador','1'))
    c = ["1000", "1", limpar_cnpj(nf.get('cnpj_forn','')), "", "1", acum, "", str(nf.get('doc','') or ""), "1", "", dt, dt, formatar_valor(nf.get('valor_total',0)), "", obs, "C", "","","","","","","","","","E"]
    return "|" + "|".join(c[:25]) + "|" + "|" * 70
def gerar_registro_1020(nf):
    v = formatar_valor(nf.get('valor_total',0))
    return f"|1020|1||{v}|0,00|0,00|0,00|0,00|0,00|0,00|{v}||||"
def gerar_registro_1300(nf, obs):
    return f"|1300|{nf.get('data','')}|55|5|{formatar_valor(nf.get('valor_total',0))}|1|{obs}|SISTEMA|"

# --- INTERFACE ---
st.set_page_config(page_title="Domínio Automator v11.3", layout="wide")
st.title("⚡ Domínio Automator - V11.3")

with st.sidebar:
    ferramenta = st.radio("Módulo:", ["📄 1. Importar PDFs", "📊 2. Confronto Excel"])
    st.markdown("---")
    cnpj_alvo = st.text_input("CNPJ Destino", value="40633348000130")
    texto_obs = st.text_input("Observação", value="IMPORTACAO AUTOMATICA")
    if st.button("🗑️ Limpar"): st.rerun()

# --- MÓDULO 1: PDF ---
if "1." in ferramenta:
    st.subheader("Extração de PDFs")
    arquivos = st.file_uploader("PDFs", type="pdf", accept_multiple_files=True)
    if arquivos and st.button("Processar"):
        notas, falhas = [], {}
        for f in arquivos:
            res, err = extrair_dados_pdf_offline(f.name, f.read(), cnpj_alvo)
            if res: notas.append(res)
            else: falhas[f.name] = err
        st.session_state.notas = notas
        if falhas: st.warning(f"Falhas em {len(falhas)} arquivos.")

    if 'notas' in st.session_state:
        df = pd.DataFrame(st.session_state.notas)
        st.dataframe(df, use_container_width=True)
        st.download_button("Baixar Excel", to_excel(df), "notas.xlsx")

# --- MÓDULO 2: CONFRONTO ---
elif "2." in ferramenta:
    st.subheader("Confronto de Acumuladores (Mês Atual vs Anterior)")
    c1, c2 = st.columns(2)
    with c1: f_atual = st.file_uploader("Excel Atual", type=["xlsx","csv"])
    with c2: f_base = st.file_uploader("Excel Anterior (Base)", type=["xlsx","csv"])

    if f_atual and f_base:
        try:
            # Novo carregador seguro que ignora cabeçalhos sujos do Domínio
            df_at = carregar_planilha_segura(f_atual)
            df_bs = carregar_planilha_segura(f_base)
            
            if 'CNPJ' in df_bs.columns and 'ACUMULADOR' in df_bs.columns:
                df_bs['CNPJ_KEY'] = df_bs['CNPJ'].apply(lambda x: limpar_cnpj(str(x)))
                df_bs = df_bs[df_bs['CNPJ_KEY'] != ""] # Remove linhas sem CNPJ extraído
                df_bs = df_bs.drop_duplicates(subset=['CNPJ_KEY']).set_index('CNPJ_KEY')
                
                # Prepara o lado atual
                col_cnpj_at = 'cnpj_forn' if 'cnpj_forn' in df_at.columns else 'CNPJ'
                if col_cnpj_at in df_at.columns:
                    df_at['cnpj_key'] = df_at[col_cnpj_at].apply(lambda x: limpar_cnpj(str(x)))
                    df_at['acumulador_anterior'] = df_at['cnpj_key'].map(df_bs['ACUMULADOR']).fillna("NÃO ENCONTRADO")
                else:
                    df_at['acumulador_anterior'] = "NÃO ENCONTRADO"
                
                # Sugere o acumulador baseado no anterior
                def sugerir_acum(v):
                    try: return str(int(float(v)))
                    except: return "1"
                df_at['acumulador'] = df_at['acumulador_anterior'].apply(sugerir_acum)
                
                st.info("💡 Compare as colunas abaixo. Você pode editar a coluna 'Acumulador (PARA IMPORTAR)'.")
                
                # Configuração da visualização lado a lado
                cols_view = [c for c in ['doc', 'cnpj_forn', 'acumulador_anterior', 'acumulador', 'valor_total', 'data'] if c in df_at.columns]
                
                df_final = st.data_editor(
                    df_at[cols_view],
                    column_config={
                        "acumulador_anterior": st.column_config.TextColumn("🔍 Acumulador (MÊS ANTERIOR)", disabled=True),
                        "acumulador": st.column_config.TextColumn("✏️ Acumulador (PARA IMPORTAR)"),
                        "cnpj_forn": st.column_config.TextColumn("Fornecedor", disabled=True),
                        "valor_total": st.column_config.NumberColumn("Valor", format="%.2f", disabled=True)
                    },
                    hide_index=True, use_container_width=True
                )
                
                if st.button("💾 Gerar TXT Domínio"):
                    buf = [gerar_registro_0000(cnpj_alvo)]
                    for _, nf in df_final.iterrows():
                        n = nf.to_dict()
                        buf.extend([gerar_registro_1000(n, texto_obs), gerar_registro_1020(n), gerar_registro_1300(n, texto_obs)])
                    st.download_button("Baixar TXT", "\r\n".join(buf), "importacao.txt")
            else:
                st.error("❌ O sistema não conseguiu extrair as colunas 'CNPJ' ou 'ACUMULADOR'. Verifique se o relatório está correto.")
        except Exception as e: st.error(f"Erro: {e}")
