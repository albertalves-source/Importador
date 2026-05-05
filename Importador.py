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
        if "AC." in valores or "ACUMULADOR" in valores or "CNPJ" in valores or "FORNECEDOR" in valores:
            idx_header = i
            break
            
    df = df_temp.iloc[idx_header+1:].copy()
    colunas_brutas = [str(c).strip().upper() for c in df_temp.iloc[idx_header].values]
    
    # Limpa nomes bizarros (UNNAMED, NAN, etc)
    colunas_limpas = []
    for i, c in enumerate(colunas_brutas):
        if c in ['NAN', 'NONE', '']: colunas_limpas.append(f"COL_{i}")
        else: colunas_limpas.append(c)
    df.columns = colunas_limpas
    
    # Padronização MÁXIMA do layout do Domínio para o nosso sistema
    for col in df.columns:
        if col in ['AC.', 'ACUMULADOR']: df.rename(columns={col: 'acumulador'}, inplace=True)
        elif col in ['NOTA', 'DOC']: df.rename(columns={col: 'doc'}, inplace=True)
        elif col in ['DATA']: df.rename(columns={col: 'data'}, inplace=True)
        elif 'VALOR CONT' in col or col == 'VALOR_TOTAL': df.rename(columns={col: 'valor_total'}, inplace=True)
        elif col == 'FORNECEDOR': df.rename(columns={col: 'nome_fornecedor'}, inplace=True)
        elif col in ['CNPJ', 'CNPJ_FORN']: df.rename(columns={col: 'cnpj_forn'}, inplace=True)
    
    # Se o Domínio não gerou coluna CNPJ, tenta extrair do nome do Fornecedor
    if 'cnpj_forn' not in df.columns:
        if 'nome_fornecedor' in df.columns:
            def extrair_cnpj(texto):
                nums = limpar_cnpj(str(texto))
                return nums if len(nums) >= 11 else ""
            df['cnpj_forn'] = df['nome_fornecedor'].apply(extrair_cnpj)
        else:
            df['cnpj_forn'] = ""
            
    # Assegurar que os valores financeiros são números formatados corretamente
    if 'valor_total' in df.columns:
        def to_float(x):
            if pd.isna(x): return 0.0
            s = str(x).strip()
            if ',' in s and '.' in s: s = s.replace('.', '').replace(',', '.')
            elif ',' in s: s = s.replace(',', '.')
            try: return float(s)
            except: return 0.0
        df['valor_total'] = df['valor_total'].apply(to_float)
        
    # Limpeza de lixo gerado nos relatórios Domínio (linhas de "Total Acumulador" ou em branco)
    if 'nome_fornecedor' in df.columns:
        df = df[~df['nome_fornecedor'].astype(str).str.upper().isin(['NAN', 'NONE', 'TOTAL ACUMULADOR', ''])]
        df = df.dropna(subset=['nome_fornecedor'])

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
        
        cnpj_alvo = limpar_cnpj(cnpj_destino_usuario)
        docs = list(dict.fromkeys(re.findall(r'\d{14}|\d{11}', texto_denso)))
        for d in docs:
            if d != cnpj_alvo and d != "00000000000000" and not d.startswith("25155"):
                dados["cnpj_forn"] = d
                break
        if not dados["cnpj_forn"]: dados["cnpj_forn"] = docs[0] if docs else "00000000000000"

        doc_str = None
        for p in [r"NFS-E[^\d]{0,30}?0*(\d+)", r"NUMERO[^\d]{0,50}?0*(\d+)", r"NF-?E?\s*[:.-]?\s*0*(\d+)"]:
            m = re.findall(p, texto_limpo)
            validos = [x for x in m if x not in ['2024','2025','2026','0']]
            if validos: {doc_str := validos[0]}; break
        
        if not doc_str:
            nums = re.findall(r'\d+', file_name)
            if nums: doc_str = max(nums, key=len)

        dados["doc"] = int(doc_str) if doc_str else 1
        
        dt = re.search(r"(\d{2}/\d{2}/\d{4})", texto_denso)
        dados["data"] = dt.group(1) if dt else datetime.now().strftime("%d/%m/%Y")

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
st.set_page_config(page_title="Domínio Automator v11.4", layout="wide")
st.title("⚡ Domínio Automator - V11.4")

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
            df_at = carregar_planilha_segura(f_atual)
            df_bs = carregar_planilha_segura(f_base)
            
            if 'acumulador' in df_bs.columns:
                # Cria chaves de busca duplas (Por CNPJ e Por NOME)
                df_bs['match_cnpj'] = df_bs.get('cnpj_forn', pd.Series(dtype=str)).apply(lambda x: limpar_cnpj(str(x)))
                df_bs['match_nome'] = df_bs.get('nome_fornecedor', pd.Series(dtype=str)).apply(lambda x: str(x).strip().upper())
                
                map_by_cnpj = df_bs[df_bs['match_cnpj'] != ""].drop_duplicates('match_cnpj').set_index('match_cnpj')['acumulador'].to_dict()
                map_by_nome = df_bs[df_bs['match_nome'] != "NAN"].drop_duplicates('match_nome').set_index('match_nome')['acumulador'].to_dict()
                
                def buscar_acumulador(row):
                    cnpj = limpar_cnpj(str(row.get('cnpj_forn', '')))
                    nome = str(row.get('nome_fornecedor', '')).strip().upper()
                    
                    if cnpj and cnpj in map_by_cnpj: return map_by_cnpj[cnpj]
                    if nome and nome in map_by_nome: return map_by_nome[nome]
                    return "NÃO ENCONTRADO"
                
                df_at['acumulador_anterior'] = df_at.apply(buscar_acumulador, axis=1)
                
                def sugerir_acum(v):
                    try: return str(int(float(v)))
                    except: return "1"
                df_at['acumulador'] = df_at['acumulador_anterior'].apply(sugerir_acum)
                
                st.info("💡 Compare as colunas abaixo. Você pode editar a coluna 'AC (Novo)'.")
                
                # Configuração da visualização Bonita e Completa
                cols_view = [c for c in ['doc', 'nome_fornecedor', 'cnpj_forn', 'acumulador_anterior', 'acumulador', 'valor_total', 'data'] if c in df_at.columns]
                
                df_final = st.data_editor(
                    df_at[cols_view],
                    column_config={
                        "acumulador_anterior": st.column_config.TextColumn("🔍 AC (Mês Ant.)", disabled=True),
                        "acumulador": st.column_config.TextColumn("✏️ AC (Novo)"),
                        "nome_fornecedor": st.column_config.TextColumn("Fornecedor", disabled=True),
                        "cnpj_forn": st.column_config.TextColumn("CNPJ", disabled=True),
                        "doc": st.column_config.TextColumn("Nota", disabled=True),
                        "valor_total": st.column_config.NumberColumn("Valor R$", format="%.2f", disabled=True),
                        "data": st.column_config.TextColumn("Data", disabled=True)
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
                st.error("❌ O sistema não conseguiu achar a coluna do 'Acumulador' na planilha base.")
        except Exception as e: st.error(f"Erro no processamento: {e}")
