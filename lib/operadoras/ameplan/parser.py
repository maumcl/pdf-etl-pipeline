from lib.legado.legado import *
from lib.df_functions import *
from .specifics import *
def parser():
    return {
        "rg": {
            "read_params": {
                "v1": {
                    "columns": {
                        "data": ["0"],
                        "faturamento": ["1"],
                        "vidas": ["2"],
                        "sinistro": ["9"],
                        "sinistralidade": ["12"],
                        "bkp": ["3"],
                        "bkp2": ["10"],
                    }
                }
            },
            "etl": {
                "v1": [
                    {"function": parse_float},
                    {"function": rg_v1_fix_valor_dac_abi},
                    {"function": rg_v1_parse_date},
                ]
            }
        },
        "ben": {
            "read_params": {
                "v1": {
                    "columns": {
                        "nome_plano": ["Código do Plano"],
                        "cod_associado": ["Código do Beneficiário"],
                        "nome": ["Nome do Beneficiário"],
                        "titularidade": ["Grau Parentesco"],
                        "inclusao_plano_real": ["DT_Inclusão"],
                        "cancelamento_plano_real": ["Data_Cancelamento"],
                        "status": ["STATUS_ATUAL"],
                        "cpf_real": ["CPF"],
                        "subestipulante": ["COD_EMPRESA"],
                        "nascimento": ["Data de Nascimento"],
                        "sexo": ["Sexo"],
                        "estado_civil": ["Estado Cívil"],
                        "bkp": ["Nome da Mãe"],
                        "logradouro": ["ENDEREÇO"],
                        "bairro": ["BAIRRO"],
                        "cidade": ["CIDADE"],
                        "estado": ["ESTADO"]

                    }
                }
            },
            "etl": {
                "v1": [
                    {"function": parse_date},
                    {"function": parse_titularidade},
                    {"function": ben_v1_parentesco},
                    {"function": parse_competencia},
                    {"function": fix_status_v1},
                    {"function": remove_tracos_e_pontos},
                    {"function": get_matricula_familia},
                    {"function": fix_plano_v1},
                    {"function": fix_data},
                    {"function": ben_v2_get_cpf_nome_titular},
                    {"function": apply_new_md5},
                    {"function": Aplicar_DEV_Filtros},

                ]
            }
        },
        "ev": {
            "read_params": {
                "v1": {
                    "columns": {
                        "competencia": ["MÊS_COMPETENCIA"],
                        "cnpj_subestipulante": ["CÓD_EMP"],
                        "subestipulante": ["EMPRESA"],
                        "cod_associado": ["CÓDIGO_BENEFICIÁRIO"],
                        "titularidade": ["Código de Dependente"],
                        "nome_beneficiario": ["NOME_BENEFICIÁRIO"],
                        "codigo_procedimento": ["CÓDIGO_PROCEDIMENTO"],
                        "descricao": ["DESCRIÇÃO_PROCEDIMENTO"],
                        "subdetalhamento": ["DESCRICAO_TIPO_EVENTO"],
                        "cod_executor": ["Código do Prestador"],
                        "executor": ["NOME_PRESTADOR"],
                        "data_inicio": ["DATA_EVENTO"],
                        "data_pagamento": ["DATA_PAGAMENTO"],
                        #"valor": ["VALOR_SINISTRO"],
                        "valor": ["VALOR_SINISTRO_PAGO"],
                        "guia": ["GUIA_PAGTO_SINISTRO"],
                        "grupo_tipo_atendimento": ["TIPO_GUIA"],
                        "cpf_real": ["CPF"],
                        "sexo": ["SEXO"],
                        "cod_plano": ["CODIGO_PLANO"],
                        "plano": ["DESCRICAO_PLANO"],
                        "descricao_subgrupo": ["TIPO_ATENDIMENTO"],
                        "": [""],



                    }
                }
            },
            "etl": {
                "v1": [
                     {"function": parse_date},
                     {"function": fix_titularidade_evs},
                     {"function": parse_titularidade},
                     {"function": remove_tracos_e_pontos},
                     {"function": get_matricula_familia},
                     {"function": parse_categoria_ameplan},
                     {"function": parse_float},
                     {"function": apply_new_md5},
                     {"function": clean_cod_exe},
                     {"function": pre_tabela_mae},
                     {"function": Aplicar_Tabela_Mae, "params":{"segmentar_exames": False} },
                     {"function": pos_tabela_mae},
                    {"function": Aplicar_DEV_Filtros},
                    {"function": transpose_nome_cpf_from_db_v1},
                    {"function": ev_v1_amplan_fix_dif_sinistro},
                ]
            }
        },
        "men": {
            "read_params": {
                "v1": {
                    "columns": {
                        "": [""],
                        "": [""],
                        "": [""],
                        "": [""],
                        "": [""],
                        "": [""],
                        "": [""],
                        "": [""],
                        "": [""],
                        "": [""],
                    }
                }
            },
            "etl": {
                "v1": [
                    #{"function": , "params": {}, "company_ignore": [], "company_only": []},
                    #{"function": debug},
                    #{"function": },
                    #{"function": parse_cpf_from_db, "params": { "force_md5": False}},
                    #{"function": }
                ]
            }
        }
    }
