import pandas as pd
import numpy as np
from lib.legado.legado import *
from lib.df_functions import *

def rg_v1_parse_date(df, **kwargs):
    df["data"] = df["data"].apply(lambda x: pd.to_datetime(x.replace("-", "") + "01"))
    return df

def rg_v1_fix_valor_dac_abi(df, **kwargs):
    if 'bkp' in df.columns and 'bkp2' in df.columns:
        df['bkp'] = df['bkp'].apply(lambda x: str(x).replace('R$ ','').replace('.','').replace(',','.') if 'nan' != str(x) else x)
        df['bkp2'] = df['bkp2'].apply(lambda x: str(x).replace('R$ ','').replace('.','').replace(',','.') if 'nan' != str(x) else x)
        df['bkp'] = df['bkp'].fillna(0)
        df['bkp2'] = df['bkp2'].fillna(0)
        df['bkp'] = df['bkp'].astype(float)
        df['bkp2'] = df['bkp2'].astype(float)
        df['bkp3'] = df['bkp'] + df['bkp2']
    return df

def fix_status_v1 (df, **kwargs):
    df['status'] = df['status'].str.replace("ATIVO", "Ativo")
    df['status'] = df['status'].str.replace("DESLIGADO", "Não ativo")
    return df

def get_matricula_familia (df, **kwargs):
    df['matricula_familia'] = df['cod_associado'].str[:14]
    return df



def fix_titularidade_evs (df, **kwargs):
    df['titularidade'] = df['titularidade'].str.replace("1", "dependente")
    df['titularidade'] = df['titularidade'].str.replace("0", "titular")
    return df

def parse_categoria_ameplan(df, **kwargs):
    def categorizar(df):

        tipo_guia = str(df["grupo_tipo_atendimento"])
        tipo_atendimento = str(df["descricao_subgrupo"])

        if "Resumo de Internação" in tipo_guia:
            return 'Internação'

        if "SADT" in tipo_guia and 'PRONTO ATENDIMENTO' in tipo_atendimento:
            return 'Consulta em pronto socorro'

        return None

    df['categoria'] = df.apply(categorizar, axis=1)
    return df

def clean_cod_exe (df, **kwargs):
    df['cod_executor'] = df['cod_executor'].str.replace(".", "")
    df['cod_executor'] = df['cod_executor'].str.replace("/", "")
    df['cod_executor'] = df['cod_executor'].str.replace("-", "")

    return df

planocod_to_planostr = {
    '95-AMP 160E PJ' : 'AMP 160E PJ',
    '96-AMP 260E PJ' : 'AMP 260E PJ',
    '97-AMP 360E PJ' : 'AMP 360A PJ',
    '98-AMP 360A PJ' : 'AMP 360E PJ',
}

def fix_plano_v1 (df, **kwargs):
    for key in planocod_to_planostr:
        df['nome_plano'] = df['nome_plano'].str.replace(key, planocod_to_planostr[key])
    return df

def fix_data (df, **kwargs):
    df['inclusao_plano'] = df['inclusao_plano_real']
    df['cancelamento_plano'] = df['cancelamento_plano_real']

    return df

def ben_v1_parentesco(df, **kwargs):
    if "titularidade" in df.columns:
        df['parentesco'] = df['titularidade']
        df['titularidade'] = np.where(df['titularidade']!='titular', 'dependente', 'titular')
    return df

def ben_v2_get_cpf_nome_titular(df, **kwargs):
    if "cpf_real_titular" not in df.columns:
        df['cpf_real_titular'] = None
    if "nome_titular" not in df.columns:
        df['nome_titular'] = None

    df['cpf_real_titular'] = np.where(df['titularidade']=='titular', df['cpf_real'], df['cpf_real_titular'])
    df['nome_titular'] = np.where(df['titularidade']=='titular', df['nome'], df['nome_titular'])

    titular = df[df['titularidade'] == 'titular']
    titular.drop_duplicates(subset=['matricula_familia'], inplace=True)

    dict_tit_bens1 = dict(zip(titular['matricula_familia'], titular['cpf_real']))
    df['cpf_real_titular'] = df['cpf_real_titular'].fillna(df['matricula_familia'].map(dict_tit_bens1))

    dict_tit_bens2 = dict(zip(titular['matricula_familia'], titular['nome']))
    df['nome_titular'] = df['nome_titular'].fillna(df['matricula_familia'].map(dict_tit_bens2))
    return df


def pre_tabela_mae(df, **kwargs):
    df['categoria'] = df['grupo_tipo_atendimento'].map({
        'Consulta': 'Consulta Eletiva',
        'Resumo de Internação': 'Internacao',
    })
    regras_categoria = {
        ('SADT', 'BIOLOGIA MOLECULAR'): 'Exames',
        ('SADT', 'BIOQUIMICA'): 'Exames',
        ('SADT', 'ECG - TE'): 'Exames',
        ('SADT', 'ENDOCRINOLOGIA LABORATORIAL - COM DIRETRIZ DE UTIL'): 'Exames',
        ('SADT', 'ENDOSCOPIA DIAGNOSTICA'): 'Exames',
        ('SADT', 'ENDOSCOPIA INTERVENCIONISTA'): 'Exames',
        ('SADT', 'IMUNOLOGIA'): 'Exames',
        ('SADT', 'HEMATOLOGIA LABORATORIAL'): 'Exames',
        ('SADT', 'LIQUIDOS - CEFALORRAQUEANO - LIQUOR SEMINAL AMNIOT'): 'Exames',
        ('SADT', 'MATERIAIS'): 'Outros Procedimentos Ambulatoriais',
        ('SADT', 'MICROBIOLOGIA'): 'Exames',
        ('SADT', 'MIGRACAO'): 'consulta em pronto socorro',
        ('SADT', 'ONOGRAFIA - ULTRA-SONOGRAFIA DIAGN'): 'Exames',
        ('SADT', 'RADIOGRAFIAS - BACIA E MEMBROS INFERIORES'): 'Exames',
        ('SADT', 'RADIOGRAFIAS - ESQUELETO TORACICO E MEMBROS SUPER'): 'Exames',
        ('SADT', 'RADIOGRAFIAS - OUTROS EXAMES'): 'Exames',
        ('SADT', 'REABILITACOES/SESSOES'): 'Outros Procedimentos Ambulatoriais',
        ('SADT', 'RESSONANCIA MAGNTICA DIAGNOSTICA'): 'Exames',
        ('SADT', 'TOMOGRAFIA COMPUTADORIZADA DIAGNOS'): 'Exames',
        ('SADT', 'URINALISE'): 'Exames',
    }

    for (categoria_original, subcategoria), categoria in regras_categoria.items():
        mask = (df['grupo_tipo_atendimento'] == categoria_original) & (df['subdetalhamento'] == subcategoria)
        df.loc[mask, 'categoria'] = categoria

    return df

def pos_tabela_mae(df, **kwargs):
    mask = df['categoria'].isnull()
    df.loc[mask, 'categoria'] = df.loc[mask, 'grupo_tipo_atendimento'].map({
        'Null': 'Outros Procedimentos Ambulatoriais',
    })

    regras_categoria = {
        ('SADT', 'BRASINDICE GERAL'): 'Outros Procedimentos Ambulatoriais',
        ('SADT', 'CONSULTA'): 'Consulta Eletiva',
        ('SADT', 'CONSULTAS, VISITAS HOSPITALARES OU'): 'Outros Procedimentos Ambulatoriais',
        ('SADT', 'COPROLOGIA'): 'Exames',
        ('SADT', 'MONITORIZACOES'): 'Exames',
        ('SADT', 'OUTROS'): 'Outros Procedimentos Ambulatoriais',
        ('SADT', 'PROCEDIMENTOS'): 'Exames',
        ('SADT', 'PROCEDIMENTOS - COM DIRETRIZ DE UTILIZACAO'): 'Exames',
        ('SADT', 'SIMPRO GERAL'): 'Outros Procedimentos Ambulatoriais',
    }

    for (categoria_original, subcategoria), categoria in regras_categoria.items():
        mask = (df['grupo_tipo_atendimento'] == categoria_original) & (df['subdetalhamento'] == subcategoria) & (df['categoria'].isnull())
        df.loc[mask, 'categoria'] = categoria

    return df


def ev_v1_amplan_fix_dif_sinistro(evs, **kwargs):
    db = kwargs.get('db', None)
    evs = evs[~evs['competencia'].isnull()]
    empresa_id = evs['empresa_id'].unique()[0]
    operadora = evs['operadora'].unique()[0]
    merged = db.runQueryWithPandas(
        f"SELECT data, sinistro, bkp, bkp2 FROM BI_Validador WHERE empresa_id = {evs.empresa_id.unique()[0]} AND operadora LIKE '%%{evs.operadora.unique()[0]}%%' AND data >= '{str(pd.to_datetime(min(evs.competencia.unique()))).replace(' 00:00:00', '')}' AND data <= '{str(pd.to_datetime(max(evs.competencia.unique()))).replace(' 00:00:00', '')}'"
    )
    merged.rename(columns={"data": "competencia"}, inplace=True)
    merged.rename(columns={"sinistro": "sinistro_validador"}, inplace=True)
    merged.rename(columns={"bkp": "dac"}, inplace=True)
    merged.rename(columns={"bkp2": "abi(sus)"}, inplace=True)
    temp = evs.groupby("competencia")["valor"].sum().reset_index()
    temp.rename(columns={"valor": "sinistro_arquivo"}, inplace=True)

    #ADD RCODE DO DAC - BKP
    df = pd.merge(temp, merged, on="competencia")
    df_dac = df.copy()
    df_dac = df_dac[df_dac['dac']!=0.0]
    del df_dac['abi(sus)']
    df_dac.rename(columns={"dac": "valor"}, inplace=True)
    df_dac['rcode'] = '666'
    df_dac['categoria'] = 'dac'
    df_dac['empresa_id'] = empresa_id
    df_dac['operadora'] = operadora
    del df_dac['sinistro_validador']
    del df_dac['sinistro_arquivo']
    #print('df_dac')
    #display(df_dac)

    #ADD RCODE DO ABI(SUS) - BKP2
    df_abi = df.copy()
    df_abi = df_abi[df_abi['abi(sus)']>0]
    del df_abi['dac']
    df_abi.rename(columns={"abi(sus)": "valor"}, inplace=True)
    df_abi['rcode'] = '666'
    df_abi['categoria'] = 'abi(sus)'
    df_abi['empresa_id'] = empresa_id
    df_abi['operadora'] = operadora
    del df_abi['sinistro_validador']
    del df_abi['sinistro_arquivo']
    #print('df_abi')
    #display(df_abi)

    evs = pd.concat([evs, df_dac, df_abi], axis=0)
    #print('evs')
    #display(evs)
    temp = evs.groupby("competencia")["valor"].sum().reset_index()
    temp.rename(columns={"valor": "sinistro_arquivo"}, inplace=True)
    df = pd.merge(temp, merged, on="competencia")

    #ADD DIF SINISTRO - VALIDADOR
    df['sinistro_validador'] = df['sinistro_validador'].astype(float)
    df['sinistro_arquivo'] = df['sinistro_arquivo'].astype(float)
    df['valor'] = round((df['sinistro_validador'] - df['sinistro_arquivo']),3)
    df['rcode'] = '666'
    df['empresa_id'] = empresa_id
    df['operadora'] = operadora
    del df['dac']
    del df['abi(sus)']
    del df['sinistro_arquivo']
    del df['sinistro_validador']
    #print('DF')
    #display(df)

    if "rcode" not in evs.columns:
        evs['rcode'] = None

    if "subestipulante" not in evs.columns:
        evs['subestipulante'] = None

    evs = pd.concat([evs, df], axis=0)
    return evs

def transpose_nome_cpf_from_db_v1(df, **kwargs):
    db = kwargs.get("db")
    df = df[~df['competencia'].isnull()]
    df['key'] = df['cod_associado'] + df['cpf_real']
    #display(df)
    base = db.runQueryWithPandas(f"""SELECT nome AS nome_beneficiario, cpf, cpf_real, nome_titular, cpf_titular, cpf_real_titular, cod_associado
                                                        FROM (SELECT nome, cpf, cpf_real, nome_titular, cpf_titular, cpf_real_titular, cod_associado
                                                                        FROM BI_Beneficiario
                                                                        WHERE empresa_id = {df.empresa_id.unique()[0]}
                                                                            AND operadora LIKE '%{df.operadora.unique()[0]}%'
                                                                            AND cod_associado IN ({", ".join(["'" + str(x) + "'" for x in df['cod_associado'].unique()])})
                                                                         GROUP BY nome, cpf, cpf_real, nome_titular, cpf_titular, cpf_real_titular, cod_associado
                                                                     UNION
                                                                     SELECT nome AS nome_beneficiario, cpf, cpf_real, nome_titular, cpf_titular, cpf_real_titular, cod_associado
                                                                        FROM BI_Colaborador
                                                                        WHERE empresa_id = {df.empresa_id.unique()[0]}
                                                                            AND operadora LIKE '%{df.operadora.unique()[0]}%'
                                                                            AND cod_associado IN ({", ".join(["'" + str(x) + "'" for x in df['cod_associado'].unique()])})
                                                                         GROUP BY nome, cpf, cpf_real, nome_titular, cpf_titular, cpf_real_titular, cod_associado) a GROUP BY nome, cpf, cpf_real, nome_titular, cpf_titular, cpf_real_titular, cod_associado""")
    base['key'] = base['cod_associado'] + base['cpf_real']
    #display(base)

    for coluna in ['nome_beneficiario', 'cpf', 'cpf_real', 'cpf_titular', 'cpf_real_titular', 'nome_titular', 'cod_associado']:
        if coluna in df.columns:
            del df[f'{coluna}']
            df = df.merge(base[['key', coluna]], how = 'left', on = 'key')
        else:
            df = df.merge(base[['key', coluna]], how = 'left', on = 'key')
    del df['key']
    return df