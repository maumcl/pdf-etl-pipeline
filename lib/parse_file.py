import json5
import glob
import pandas as pd
import numpy as np
import inspect
from glob import glob
from tqdm.notebook import tqdm
from functools import reduce
from lib import df_functions
from lib import homonimos
from lib import operadoras as operadoras
from lib.checkpoints import checkpoint_ben, checkpoint_ev, checkpoint_men, checkpoint_cop
from lib.df_functions import fix_mensalidades,check_with_validador_ben, check_with_validador_evs, get_cards, cheeck_with_db
from lib.legado import legado
from lib.legado.WellbeDatabase import WellbeDatabase
from lib.legado.WellbeTest import *

dataGeneral = {}
readers = {}
finalizers = {}
def reload():
    for operadora, module in inspect.getmembers(operadoras, inspect.ismodule):
        parser = [file[1] for file in inspect.getmembers(module, inspect.ismodule) if file[0] == 'parser']
        assert len(parser) == 1, f"não foi possível achar o arquivo parser.py para a operadora {operadora}"
        parser = [function[1] for function in inspect.getmembers(parser[0], inspect.isfunction) if function[0] == 'parser'][0]()
        dataGeneral.update({operadora: parser})

        reader = [file[1] for file in inspect.getmembers(module, inspect.ismodule) if file[0] == 'reader']
        assert len(reader) == 1, f"não foi possível achar o arquivo reader.py para a operadora {operadora}"
        reader_functions = dict([(operadora + '_' + function[0], function[1]) for function in inspect.getmembers(reader[0], inspect.isfunction) if 'read' in function[1].__name__])
        readers.update(reader_functions)

        finalizer = [file[1] for file in inspect.getmembers(module, inspect.ismodule) if file[0] == 'finalizer']
        if len(finalizer) == 1:
            finalizer = [function[1] for function in inspect.getmembers(finalizer[0], inspect.isfunction) if function[0] == 'finalizer'][0]()
        finalizers.update({operadora: finalizer})
        
    
def parse_file(file_path, empresa_id, operadora, table, db=None, debug=False):
    reload()
    if operadora.lower().replace("central nacional unimed", "cnu") not in file_path.lower():
        warning(f"Atenção: operadora '{operadora}' não foi encontrada no file_path")
    if debug:
        print("Empresa_id:", empresa_id)
        print(f"Acessando dicionário: [{operadora.lower().replace(' ','_')}][{table}]")
    data = dataGeneral[operadora.lower().replace(' ','_')][table]
    if debug:
        print("Lendo arquivo com:", operadora.lower().replace(' ','_') + '_read_' + table)
    df, version = readers[operadora.lower().replace(' ','_') + '_read_' + table](file_path, **data['read_params'])
    
    df, empresa_id = df_functions.general_assign(df, db=db, file_path=file_path, empresa_id=empresa_id, operadora=operadora, debug=debug)
    
    if not df.empty:
        for step in data['etl'][version]:
            function = step['function']
            function_name = function.__name__
            if "company_ignore" in step and (int(empresa_id) in step['company_ignore'] or str(empresa_id) in step['company_ignore']):
                if debug:
                    print("Skipping (company_ignore):", function_name)
                continue
            elif "company_only" in step and (int(empresa_id) not in step['company_only'] and str(empresa_id) not in step['company_only']):
                if debug:
                    print("Skipping (company_only):", function_name)
                continue
            else:
                if debug:
                    print("Rodando:", function_name)
                if "params" in step.keys():
                    df = df.pipe(function, db=db, **step['params'])
                else:
                    df = df.pipe(function, db=db)
            if df is None:
                raise Exception("Dataframe retornou vazio após a função:", function_name)

        filtros = ['cod_cid', 'cid', 'executor', 'operadora', 'subestipulante', 'nome_plano', 'plano', 'status', 'sexo', 'titularidade', 'aux', 'lotacao', 'categoria']

        for column in filtros:
            if column in df.columns:
                df[column] = df[column].apply(lambda x: x if str(x).lower() in ['nan','none'] else str(x).lower())

    if table == 'ben':
        return df_functions.split_colab_benes(df)
    else:
        return df
    
    
def get_function(operadora, table, show_function=True):
    try:
        funcao = finalizers[operadora.lower().replace(' ','_')][table]["funcao"]
    except:
        funcao = None
    finally:
        if bool(funcao):
            print("Função:", funcao['nome'])
            if show_function:
                print("Código:", end=" ")
                codigo, _ = inspect.getsourcelines(funcao["funcao"])
                print(''.join(codigo))
        else:
            print("Não há uma função de finalização definida para a operadora: '{operadora}', tabela: '{table}'".format(operadora=operadora.lower().replace(' ','_'), table=table))
    return funcao

def get_queries(operadora, table, show_queries=True):
    try:
        queries = finalizers[operadora.lower().replace(' ','_')][table]["queries"]
    except:
        queries = []
    finally:
        if len(queries) > 0:
            for query in queries:
                print("Nome:", query['nome'])
                if show_queries:
                    print("Query:", end=" ")
                    codigo, _ = inspect.getsourcelines(query['query'])
                    print(''.join(codigo))
        else:
            print("Não há queries de finalização definidas para a operadora: '{operadora}', tabela: '{table}'".format(operadora=operadora.lower().replace(' ','_'), table=table))
    return queries

def run_function(funcao, df=None, db=None, operadora=None, empresa_id=None):
    if bool(funcao):
        print("Rodando a funcao '{nome_funcao}'".format(nome_funcao = funcao["nome"]))
        return funcao["funcao"](df=df, db=db, operadora=operadora, empresa_id=empresa_id)
    return df
    
    
def run_queries(queries, db=None, operadora=None, empresa_id=None, df=None, verbose=False, carimbo=None):
    if len(queries) > 0:
        for query in queries:
            print("Rodando a query '{nome_query}'".format(nome_query = query["nome"]))
            query["query"](df=df, db=db, operadora=operadora, empresa_id=empresa_id, verbose=verbose, carimbo=carimbo)
        print("All done!")
        