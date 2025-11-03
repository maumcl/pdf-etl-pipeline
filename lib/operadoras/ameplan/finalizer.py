import json
import tabula
import pandas as pd
from lib.legado.legado import *
from lib.df_functions import *
from .specifics import *

def finalizer():
    return {
        "rg": {
            "funcao": {
                #     "nome": "funcao_exemplo",
                #     "funcao": nome_da_funcao
            },
            "queries": [
                # {
                #     "nome": "query_exemplo",
                #     "query": nome_da_funcao
                # }
            ]
        },
        "ev": {
            "funcao": {

            },
            "queries": [{
                #"nome": "new md5 antes do transpose",
                #"query": new_md5_query
            },

                {
                #"nome": "Transpor nome e cpf titular",
                #"query": transpose_nome_e_cpf_titular
            },
            ]
        },
        "ben": {
            "funcao": {

            },
            "queries": [
            ]
        },
        "men": {
            "funcao": {
            },
            "queries": [
            ]
        }
    }
