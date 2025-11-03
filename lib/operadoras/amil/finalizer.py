import json
import tabula
import pandas as pd
from lib.legado.legado import *
from lib.df_functions import *
from .specifics import *


def ben_query_drop(df, **kwargs):
    db = kwargs.get("db")
    verbose = kwargs.get("verbose", False)
    if type(df) == list:
        df = pd.concat(df).reset_index(drop=True)
    for empresa_id in df['empresa_id'].unique():
        for operadora in df["operadora"].unique():
            print(f'Empresa: {empresa_id}')
            db.runQuery(f"""
            update BI_Colaborador t1
            left join(select    cpf_real, 
                                max(competencia) as max_c 
                    from BI_Colaborador 
                    where Empresa_id  = {empresa_id}
                    and operadora like '%{operadora}%'
                    and cpf_real is not null 
                    group by 1
                    )t2
            on t1.cpf_real = t2.cpf_real 
            set t1.empresa_id = -{empresa_id}
            where Empresa_id  = {empresa_id} and operadora like '%{operadora}%' 
            and t1.cpf_real is not null and t1.competencia < t2.max_c
        """, verbose=verbose)
        db.runQuery(f"""
            update BI_Beneficiario t1
            left join(select    cpf_real, 
                                max(competencia) as max_c 
                    from BI_Beneficiario 
                    where Empresa_id  = {empresa_id}
                     and operadora like '%{operadora}%'
                    and cpf_real is not null 
                    group by 1
                    )t2
            on t1.cpf_real = t2.cpf_real 
            set t1.empresa_id = -{empresa_id}
            where Empresa_id  = {empresa_id}
            and operadora like '%{operadora}%'
            and t1.cpf_real is not null and t1.competencia < t2.max_c
        """, verbose=verbose)


## Transpose do status Demitido/Aposentado de BI_Eventos será rodado no finalizer de Mensalidade pois temos que subir todas as bases antes de rodar ela
## Passando status de eventos do último mês para todos as tabelas e, depois, propagando para todos os meses
def ev_transpose_status(df, **kwargs):
    db = kwargs.get("db")
    verbose = kwargs.get("verbose", False)
    for empresa_id in df['empresa_id'].unique():
        for operadora in df["operadora"].unique():
            print(f'Empresa: {empresa_id}')
        db.runQuery(f"""
                update BI_Colaborador a join (select distinct a.cpf_real, status
                                  from BI_Eventos a
                                           inner join (select distinct cpf_real, max(competencia) maxcomp
                                                 from BI_Eventos
                                                 where Empresa_id = {empresa_id} and operadora like '%{operadora}%' 
                                                   and cpf_real is not null and status='Demitido.Aposentado'
                                                 group by 1) b
                                                on a.cpf_real = b.cpf_real and a.competencia = b.maxcomp
                                  where Empresa_id = {empresa_id} and operadora like '%{operadora}%' ) b on a.cpf_real = b.cpf_real
                set a.status = b.status
                where Empresa_ID = {empresa_id} and operadora like '%{operadora}%';
            """, verbose=verbose)
        db.runQuery(f"""
                update BI_Beneficiario a join (select distinct a.cpf_real, status
                                  from BI_Eventos a
                                           inner join (select distinct cpf_real, max(competencia) maxcomp
                                                 from BI_Eventos
                                                 where Empresa_id = {empresa_id} and operadora like '%{operadora}%' 
                                                   and cpf_real is not null and status='Demitido.Aposentado'
                                                 group by 1) b
                                                on a.cpf_real = b.cpf_real and a.competencia = b.maxcomp
                                  where Empresa_id = {empresa_id} and operadora like '%{operadora}%' ) b on a.cpf_real = b.cpf_real
                set a.status = b.status
                where Empresa_ID = {empresa_id} and operadora like '%{operadora}%';
            """, verbose=verbose)
        db.runQuery(f"""
                update BI_Mensalidades a join (select distinct a.cpf_real, status
                                  from BI_Eventos a
                                           inner join (select distinct cpf_real, max(competencia) maxcomp
                                                 from BI_Eventos 
                                                 where Empresa_id = {empresa_id} and operadora like '%{operadora}%'
                                                   and nome_beneficiario is not null and status='Demitido.Aposentado'
                                                 group by 1) b
                                                on a.cpf_real = b.cpf_real and a.competencia = b.maxcomp
                                  where Empresa_id = {empresa_id} and operadora like '%{operadora}%' ) b on a.cpf_real = b.cpf_real
                set a.status = b.status
                where Empresa_ID = {empresa_id} and operadora like '%{operadora}%';
            """, verbose=verbose)



def md5_nomes_empresa_finlandia(df, **kwargs):
    db = kwargs.get("db")
    if "2523" in df["empresa_id"].astype("string").unique():
        print('[2523] - Empresa Finlandia - Rodando MD5 nos nomes dos usuários')
        for tabela in ['BI_Colaborador', 'BI_Beneficiario', 'BI_Mensalidades']:
            db.runQuery(f"""
                    update {tabela}
                       set nome = md5(nome),
                           nome_titular = md5(nome_titular)
                        where Empresa_ID = 2523
                          and operadora like '%amil%';
                          """, verbose=True)
            
        for tabela in ['BI_Eventos']:
            db.runQuery(f"""
                    update {tabela}
                       set nome_beneficiario = md5(nome_beneficiario),
                           nome_titular = md5(nome_titular)
                        where Empresa_ID = 2523
                          and operadora like '%amil%';
                          """, verbose=True)

    else:
        print("skipping (company only).")


def finalizer():
    return {
        "rg": {
            "funcao": {
                
            },
            "queries": [
            ]
        },
        "ev": {
            "funcao": {
            },
            "queries": [
            ]
        },
        "ben": {
            "funcao": {
            },
            "queries": [
                {
                    "nome": "Passa beneficiários retroativos para -empresa_id",
                    "query": ben_query_drop
                }
            ]
        },
        "men": {
            "funcao": {
            },
            "queries": [

                {
                    "nome": "Transpor status de BI_Eventos para BI_Beneficiario, BI_Colaborador e BI_Mensalidades",
                    "query": ev_transpose_status
                },
                {
                    "nome": "Rodando MD5 nos nomes dos usuários da empresa Finlandia Corretora",
                    "query": md5_nomes_empresa_finlandia
                }
                
            ]
        }
    }