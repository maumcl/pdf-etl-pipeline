import pandas as pd
import numpy as np
from datetime import datetime as dt
from lib.df_functions import *
from lib.legado import *
from lib.legado.UnnimaxProcedureCategorization import UnnimaxProcedureCategorization
from lib.legado.WellbeProcedureCategorization import WellbeProcedureCategorization
from .specifics import *
from lib.legado.legado import *


def ev_v1_parse_categoria(df, **kwargs):
    if len(df['fcode'].unique()) > 1:
        raise Exception("amil_ev_parse_categoria: Não foi possível determinar o file_path")
    else:
        file_path = df['fcode'].unique()[0]
    if "unnimax" in file_path.lower():
        df['categoria'] = df.apply(
            lambda x: UnnimaxProcedureCategorization().ctg_unnimax_geral(
                x["codigo_procedimento"]),
            axis=1,
        )
    else:
        pass
    return df



def ev_v1_parse_titularidade(df, **kwargs):
    def inner(x):
        if pd.isnull(x["titularidade"]):
            if x["cpf"] == x["cpf_titular"]:
                return "Titular"
            else:
                return "Dependente"
        else:
            return x["titularidade"]
    df["titularidade"] = df.apply(inner, axis=1)
    return df

def parse_subestipulante(df, **kwargs):
    if df["empresa_id"].astype(str).unique()[0] in ("170"):
        df['subestipulante'] = df['lotacao'].astype(str) + ' - ' + df['subestipulante'].astype(str)
    return df
    
def ev_v1_parse_peona(df, **kwargs):
    competencia = df.competencia.unique()[0]
    df.loc[df["descricao"].str.lower() == "peona", "categoria"] = "peona"
    df.loc[df["descricao"].str.lower() == "peona", "titularidade"] = np.nan
    df.loc[df["descricao"].str.lower() == "peona", "sexo"] = np.nan
    df.loc[df["descricao"].str.lower() == "peona", "status"] = np.nan
    df.loc[df["descricao"].str.lower() == "peona", "plano"] = np.nan
    df.loc[df["descricao"].str.lower() == "peona", "competencia"] = competencia
    return df

def rg_v1_custom_date(df, **kwargs):
    def inner(data):
        ano = data[-4:]
        mes = data[:-5]
        if "jan" in mes.lower():
            data = ano+"-01-01"
        if "fev" in mes.lower():
            data = ano+"-02-01"
        if "mar" in mes.lower():
            data = ano+"-03-01"
        if "abr" in mes.lower():
            data = ano+"-04-01"
        if "mai" in mes.lower():
            data = ano+"-05-01"
        if "jun" in mes.lower():
            data = ano+"-06-01"
        if "jul" in mes.lower():
            data = ano+"-07-01"
        if "ago" in mes.lower():
            data = ano+"-08-01"
        if "set" in mes.lower():
            data = ano+"-09-01"
        if "out" in mes.lower():
            data = ano+"-10-01"
        if "nov" in mes.lower():
            data = ano+"-11-01"
        if "dez" in mes.lower():
            data = ano+"-12-01"
        return dt.strptime(data, "%Y-%m-%d")
    df["data"] = df["data"].apply(inner)
    return df

def rg_v4_fix_sinistralidade(df, **kwargs):
    if "sinistralidade" in df.columns:
        df['sinistralidade'] = round((df['sinistralidade']*100),2)
    return df

def ben_v1_titular_encadeado(df, **kwargs):
    df["nome_titular"] = None
    nome_titular = None

    for i in df.index:
        if pd.isnull(df["titularidade"][i]):
            df["titularidade"][i] = "Titular"
            nome_titular = df["nome"][i]
        df["nome_titular"][i] = nome_titular
    return df


def ben_v2_titular_encadeado(df, **kwargs):
    df["nome_titular"] = None
    nome_titular = None
    df["titularidade"] = None

    for i in df.index:
        if df['parentesco'][i] == '0':
            df["titularidade"][i] = "Titular"
            nome_titular = df["nome"][i]
        elif df['parentesco'][i] != '0':
            df["titularidade"][i] = "Dependente"
        df["nome_titular"][i] = nome_titular

    return df
def ben_fix_inclusao_cancelamento(df, **kwargs):
    df["cancelamento_plano_real"] = df["cancelamento_plano"]
    df["inclusao_plano_real"] = df["inclusao_plano"]
    df['inclusao_plano'] = df['inclusao_plano'].to_numpy().astype('datetime64[M]')
    df['cancelamento_plano'] = df['cancelamento_plano'] + MonthEnd(0)
    return df


def ben_v1_custom_nascimento(df, **kwargs):
    def inner(x):
        now = datetime.now()
        return x if pd.isnull(x) else now.replace(year=now.year - int(x))
    df['nascimento'] = df['nascimento'].apply(inner)
    return df

def define_ONE_NEXT_or_Amil(df, **kwargs):
    fcode = df['fcode'].unique()
    if "/one/" in str(fcode).lower():
        df['operadora'] = 'One'
    if "/next/" in str(fcode).lower():
        df['operadora'] = 'next'
    if "/amil dental" in str(fcode).lower():
        df["operadora"] = 'amil dental'
    return df

def ben_cad_v1_parse_titular(df, **kwargs):
    """
    Essa funcao está assumindo a matricula_familia (no caso o CPF_real) e setando o titular de todos os beneficiarios, após isso ela renomeia a coluna matricula_familia=cpf_real_titular

    TODO: Ajustar essa funcao futuramente
    """

    df_titular = df[df['titularidade'] == 'Titular'].copy()
    df_titular = df_titular[["matricula_familia", "cpf", "nome"]]
    df_titular = df_titular.rename(columns={'cpf':'cpf_titular', 'nome':'nome_titular'})
    df_titular.drop_duplicates(subset=["matricula_familia"], keep="last", inplace=True)
    df_titular.reset_index(inplace = True, drop=True)
    df = df.merge(df_titular, how='left', on='matricula_familia')
    return df

def set_matricula_familia(df, **kwargs):
    df['matricula_familia'] = df['bkp']
    del df['bkp']
    return df


def ben_cad_v1_drop_cancelados(df, **kwargs):
    df = df[(df.cancelamento_plano_real.isna()) | (df.cancelamento_plano_real > df.competencia)]
    if df.empty:
        raise pd.errors.EmptyDataError
    return df


def ben_cad_v1_parse_date(df, **kwargs):
    if "nascimento" in df.columns:
        df["nascimento"] = df["nascimento"].apply(lambda x: '01/01/2001' if x == '01/01/0001' else x)
    df = parse_date(df, formato="%d/%m/%Y")
    return df


def men_v1_set_sub_bkp(df, first_contrato, file_path) -> pd.DataFrame:
    """
    Procedimento para setar o subestipulante
    """
    df['bkp'] =  df['cpf'].apply(lambda x: str(x).split(":")[-1].strip() if 'Contrato:' in str(x) else np.nan)

    # Primeiro contrato antes do forward fill
    df.reset_index(inplace=True, drop=True)
    df['bkp'].iloc[0] = first_contrato

    df['bkp'] = df['bkp'].fillna(method='ffill')
    return df


def men_v1_clear_df(df, **kwargs):
    df = df[df["mensalidade"].notna()]
    df = df[~df["mensalidade"].isin(["nat", "nan", "Mensalidade"])]
    df = df[~df["cpf"].apply(lambda x: "o" in str(x))]

    df["cpf"] = df["cpf"].apply(lambda x: x if pd.isnull(x) else int(str(x).lstrip("0")))
    return df


def men_v1_parse_subestipulante_cyrela(df, **kwargs):
    df["subestipulante"] = df['fcode'].apply(lambda x: x if pd.isnull(x) else re.search(r"([0-9]{10})", 
                                        str(x).replace('\\','/')).group(0)) + " - " + df["subestipulante"]
    return df

def ev_v1_parse_replace_status(df, **kwargs):
    df["status"] = df['status'].apply(lambda x: x if pd.isnull(x) else str(x).replace('/','.'))
    return df

def men_v1_parse_datas_extras_e_status(df, **kwargs):
    for column in ["data_limite", "data_inclusao", "data_exclusao"]:
        if column in df.columns:
            df[column] = df[column].apply(lambda x: pd.NaT if pd.isnull(x) else pd.NaT if str(x).strip() == "" else pd.to_datetime(x, format='%d/%m/%Y'))
    if ("data_exclusao" in df.columns) and ("status" not in df.columns):
        assert len(df["data_realizacao"].unique()) == 1
        competencia = pd.to_datetime(df["data_realizacao"].unique()[0])
        df["status"] = df["data_exclusao"].apply(lambda x: "Ativo" if pd.isnull(x) else "Não ativo" if x <= competencia else "Ativo")
    return df

def clear_zeros(df, **kwargs):
    df["cpf_real"] = df["cpf_real"].apply(lambda x: x if pd.isnull(x) else int(str(x).lstrip("0")))
    df["cpf_real_titular"] = df["cpf_real_titular"].apply(lambda x: x if pd.isnull(x) else int(str(x).lstrip("0")))
    return df


def corr_oswaldo_cruz(df):
    # Inicialização de variáveis
    subestipulante_values = []
    current_contract = None

    # Iteração sobre as linhas do DataFrame
    for index, row in df.iterrows():
        codigo_str = str(row['Código']) if pd.notnull(row['Código']) else None  # Converte o valor para string
        if "Contrato:" in str(codigo_str):
            # Se encontrar a substring "Contrato:", salva o contrato atual
            current_contract = codigo_str
            subestipulante_values.append(current_contract)
        else:
            # Para valores que não são contratos, adiciona NaN à lista
            subestipulante_values.append(None)

    # Adiciona a lista de contratos à nova coluna no DataFrame
    df['subestipulante'] = subestipulante_values

    # Inicialização de variáveis
    current_contract = None

    # Iteração sobre as linhas do DataFrame
    for index, row in df.iterrows():
        if pd.isnull(row['Beneficiário']):
            # Se encontrar um valor nulo na coluna 'nome', reinicia o contrato atual
            current_contract = None
        elif pd.isnull(row['subestipulante']):
            # Se encontrar um valor nulo na coluna 'subestipulante', preenche com o contrato atual
            df.at[index, 'subestipulante'] = current_contract
        else:
            # Atualiza o contrato atual
            current_contract = row['subestipulante']

    df['subestipulante'] = df['subestipulante'].fillna(method='bfill')

    df['subestipulante'] = df['subestipulante'].fillna('JOANIN LOJA 24')


    # Aplica as condições e cria uma nova coluna 'categoria'
    df['subs'] = ''
    df.loc[df['subestipulante'].str.contains('COMERCIAL', case=False), 'subs'] = 'COMERCIAL OSWALDO CRUZ LTDA'
    df.loc[df['subestipulante'].str.contains('DROGARIA', case=False), 'subs'] = 'DROGARIA JOANIN'
    df.loc[df['subestipulante'].str.contains('LOJA', case=False), 'subs'] = 'JOANIN LOJA 24'
    df.loc[df['subestipulante'].str.contains('DROGARIA', case=False), 'subs'] = 'DROGARIA JOANIN'
    df.loc[df['subestipulante'].str.contains('JOANIN MATRIZ', case=False), 'subs'] = 'JOANIN MATRIZ'
    df.loc[df['subestipulante'].str.contains('MAGNO MATRIZ', case=False), 'subs'] = 'MAGNO MATRIZ'
    df.loc[df['subestipulante'].str.contains('SUPERMERCADOS JOANIN', case=False), 'subs'] = 'SUPERMERCADOS JOANIN'

    df = df[~df['Beneficiário'].isnull()]
    df = df[df['Beneficiário'] != 'Beneficiário']
    df = df[~df['Beneficiário'].str.contains('Fatura')]

    df = df.drop('subestipulante', axis = 1)

    return df


def pre_tabela_mae(df, **kwargs):
    if 'subcategoria' in df.columns:
        df['categoria'] = df['subcategoria'].map({
            'APB': 'exames',
            'APE': 'exames',
            'CEL': 'consulta eletiva',
            'CUR': 'consulta em pronto socorro',
            'DDH': 'internação',
            'DAP': 'internação',
            'DAC': 'internação',
            'DEF': 'internação',
            'GAS': 'outros procedimentos ambulatoriais',
            'MTC': 'outros procedimentos ambulatoriais',
            'MLB': 'exames',
            'MLE': 'exames',
            'MDB': 'exames',
            'MDE': 'exames',
            'MOP': 'internação',
            'DIC': 'exames',
            'DIE': 'exames',
            'RME': 'exames',
            'DIS': 'internação',
            'DBR': 'internação',
            'DAC': 'internação',
            'DPE': 'internação',
            'DUP': 'internação',
            'DUT': 'internação',
            'HDC': 'internação',
            'TCC': 'internação',
            'TCE': 'exames',
            'USE': 'exames',
            'CKP': 'exames',
            'E': 'exames',
            'MLE': 'exames',
            'MSL': 'exames',
            'CDM': 'internação',
            'THT': 'internação',
            'DSI' : 'internação',
            'TCE' : 'exames'
        })
        
    regra_categoria = {
        ('I'):'internação'
    }

    for internacao, categoria in regra_categoria.items():
        mask =(df['internacao'] == internacao)
        df.loc[mask,'categoria'] = categoria
    


    if 'guia' in df.columns:
        mask_2 = (~df['guia'].isna() & df['guia'].str.contains("QT"))
        df.loc[mask_2,'categoria'] = 'terapia'

        


    return df

def pos_tabela_mae(df, **kwargs):
    if 'subcategoria' in df.columns:
        mask = df['categoria'].isnull()
        df.loc[mask, 'categoria'] = df.loc[mask, 'subcategoria'].map({
            'MED': 'outros procedimentos ambulatoriais',
            'MCM': 'outros procedimentos ambulatoriais',
            'HFO': 'outros procedimentos ambulatoriais',
            'HNU': 'outros procedimentos ambulatoriais',
            'HPS': 'outros procedimentos ambulatoriais',
            'HNN': 'outros procedimentos ambulatoriais',
            'HFI': 'outros procedimentos ambulatoriais',
            'TAS': 'outros procedimentos ambulatoriais',
            'NA': 'outros procedimentos ambulatoriais',
            'TEQ': 'outros procedimentos ambulatoriais',
            'TUS': 'outros procedimentos ambulatoriais',
            'TEC': 'outros procedimentos ambulatoriais',
            'TEE': 'outros procedimentos ambulatoriais',
            np.nan: 'outros procedimentos ambulatoriais',
            'C': 'outros procedimentos ambulatoriais',
            'GNE': 'outros procedimentos ambulatoriais',
            'HTO': 'outros procedimentos ambulatoriais',
            'MCM': 'outros procedimentos ambulatoriais',
            'MES' : 'outros procedimentos ambulatoriais',
            'OCN' : 'outros procedimentos ambulatoriais',
            'REM' : 'outros procedimentos ambulatoriais',
            'TAD' : 'outros procedimentos ambulatoriais',
            'TGN' :'outros procedimentos ambulatoriais',
            'TXV':'outros procedimentos ambulatoriais',
            'VCN': 'outros procedimentos ambulatoriais',  
        })

        df.loc[df['categoria']=='internacao','categoria'] = 'internação'

    return df


def cancelar_futuro(df, **kwargs):
    df.loc[df['cancelamento_plano_real'] >= df['competencia'], 'cancelamento_plano'] = None
    df.loc[df['cancelamento_plano_real'] >= df['competencia'], 'status'] = 'ativo'
    return df


def transpose_titular_mensalidade(df, **kwargs):

    db = kwargs.get("db")
    return df.pipe(
            transpose,
            db=db,
            tables=["BI_Colaborador","BI_Beneficiario","BI_Mensalidades"],
            left_columns=[
                "nome","nome_titular","cpf_real_titular"
            ],
            right_columns=[
                "nome","nome_titular","cpf_real_titular"
            ],
            left_keys=["cpf_real"],
            right_keys=["cpf_real"]
        )


def transpose_sexo_mensalidade(df, **kwargs):

    db = kwargs.get("db")
    return df.pipe(
            transpose,
            db=db,
            tables=["BI_Colaborador","BI_Beneficiario","BI_Mensalidades"],
            left_columns=[
                "sexo"
            ],
            right_columns=[
                "sexo"
            ],
            left_keys=["nome"],
            right_keys=["nome"]
        )


def transpose_sexo_mensalidade(df, **kwargs):

    db = kwargs.get("db")
    return df.pipe(
            transpose,
            db=db,
            tables=["BI_Colaborador","BI_Beneficiario","BI_Mensalidades"],
            left_columns=[
                "sexo"
            ],
            right_columns=[
                "sexo"
            ],
            left_keys=["nome"],
            right_keys=["nome"]
        )



def rg_finalize(df, **kwargs):

    if "sinistralidade" not in df.columns:
        return df
    df["bkp"] = df["sinistralidade"]
    df["data"] = df["data"].apply(lambda x: str(x)[:10]) # evita bug
    df["data"] = pd.to_datetime(df["data"]) 
    
    return df

def definindo_cpf_real (df,**kwargs):

   
    df.loc[df['titularidade']=='titular','cpf_real_titular'] =df['cpf_real'].astype(str)
    df.loc[df['titularidade']=='titular','nome_titular'] =df['nome'].astype(str)
    df.loc[df['titularidade']=='titular','cpf_titular'] =df['cpf'].astype(str)
    

    return df

def fix_data_dental(df,**kwargs):


    df['nascimento'] = pd.to_datetime(df['nascimento'], format='%d/%m/%Y')
    df['nascimento'] = df['nascimento'].dt.strftime('%Y-%m-%d')

    df['competencia'] = df["competencia"].apply(lambda x:'01/'+x) 
    df['competencia'] = pd.to_datetime(df['competencia'], format='%d/%m/%Y')
    df['competencia'] = df['competencia'].dt.strftime('%Y-%m-%d')

    df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y')
    df['data'] = df['data'].dt.strftime('%Y-%m-%d')

    df['data_pagamento'] = df["data_pagamento"].apply(lambda x:'01/'+x)
    df['data_pagamento'] = pd.to_datetime(df['data_pagamento'], format='%d/%m/%Y')
    df['data_pagamento'] = df['data_pagamento'].dt.strftime('%Y-%m-%d')

    return df

def fix_valor_dental(df,**kwargs):

    df['valor']=df['valor'].str.replace(',','.').astype(float)

    return df

def concatenar_subestipulante(df, **kwargs):
 
    df["subestipulante"] = df[['bkp', 'bkp2']].agg('-'.join, axis=1)

    return df


def definindo_titular_dental (df,**kwargs):


    df.loc[df['titularidade']=='titular','nome_titular'] =df['nome_beneficiario'].astype(str)
    df.loc[df['titularidade']=='titular','cpf_titular'] =df['cpf'].astype(str)
    

    return df