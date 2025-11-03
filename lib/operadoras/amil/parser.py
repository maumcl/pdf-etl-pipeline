from lib.legado.legado import *
from lib.df_functions import *
from .specifics import *

def parser():
    return {
        "rg": {
            "read_params": {
                "v1": {
                  "columns": {
                    "data": ["Data de Competência", "MÊS"],
                    "faturamento": ["Receita", "PREMIO TOTAL"],
                    "sinistro": ["Custo Total", "SINISTRO TOTAL","Custo Odontológico"],
                    "sinistralidade": ["Sinistralidade", "% sinistralidade"],
                    "vidas": ["Qtde de Vidas", "ATIVOS"],
                    "subestipulante": ["SUBFATURA"],
                  }
                },
                "v2": {
                  "columns": {
                    "data": ["Data de Competência"],
                    "faturamento": ["Receita"],
                    "sinistro": ["Custo Total"],
                    "sinistralidade": ["Sinistralidade"],
                    "vidas": ["Qtde de Vidas"]
                  },
                },
                "v3": {
                    "columns": {
                        "data": ["Data de Competência"],
                        "faturamento": ["Receita"],
                        "subestipulante": ["Empresa Matriz"],
                        "sinistro": ["Custo Total"],
                        "sinistralidade": ["Sinistralidade"],
                        "vidas": ["Qtde de Vidas"]
                    }
                },
                "v4": {
                    "columns": {
                        "data": ["MÊS"],
                        "faturamento": ["PREMIO TOTAL"],
                        "sinistro": ["SINISTRO TOTAL"],
                        "sinistralidade": ["% sinistralidade"],
                        "vidas": ["ATIVOS"],
                        "subestipulante": ["SUBFATURA"],
                    }
                }
            },
            "etl": {
                "v1": [
                    {"function": parse_float,
                     "params": {
                         "round_values": 2,
                         "multiply_sinistralidade": True
                      }
                    },
                    {"function": rg_v1_custom_date},
                    {"function": fix_vidas_ponto},
                    {"function": define_ONE_NEXT_or_Amil},
                    {"function":lower_names_filters},
                    {"function": rg_finalize},
                ],
                "v2": [
                    {"function": parse_int},
                    {"function": parse_float},
                    {"function": rg_v1_custom_date},
                    {"function": fix_vidas_ponto},
                    {"function": define_ONE_NEXT_or_Amil},
                    {"function":lower_names_filters},
                    {"function": rg_finalize},
                ],
                "v3": [
                    {"function": parse_float,
                     "params": {
                         "round_values": 2,
                         "multiply_sinistralidade": True
                      }
                    },
                    {"function": rg_v1_custom_date},
                    {"function": fix_vidas_ponto},
                    {"function": define_ONE_NEXT_or_Amil},
                    {"function":lower_names_filters},
                    {"function": rg_finalize},
                ],
                "v4": [
                    {"function": parse_int},
                    {"function": parse_float},
                    {"function": parse_date},
                    {"function": fix_vidas_ponto},
                    {"function": rg_v4_fix_sinistralidade},
                    {"function": Aplicar_DEV_Filtros},
                    {"function": define_ONE_NEXT_or_Amil},
                    {"function":lower_names_filters},
                    {"function": rg_finalize},
                ]
            }
        },
        "ben": {
            "read_params": {
                "v1": {
                  "columns": {
                    "cod_associado": ["Código"],
                    "cpf_real": ["CPF"],
                    "nome": ["Beneficiário"],
                    "nome_plano": ["Plano"],
                    "nascimento": ["Idade"],
                    "cpf": ["Código"],
                    "titularidade": ["Dependência"],
                    "inclusao_plano": ["Data Inclusão"],
                    "cancelamento_plano": ["Data Exclusão"],
                    "subestipulante": ["subs"]
                  }
                },
                "benef_v1": {
                  "columns": {
                    "bkp": ["CONTRATO"],
                    "bkp2": ["CPF"],
                    "data_de_admissao": ["DATA DE ADMISSÃO"],
                    "nome": ["NOME DO BENEFICIÁRIO"],
                    "nascimento": ["DATA DE NASCIMENTO"],
                    "sexo": ["SEXO"],
                    "estado_civil": ["ESTADO CIVIL"],
                    "parentesco": ["PARENTESCO"],
                    "cpf": ["NR DO CARTÃO DO BENEFICIÁRIO"],
                    "cod_plano": ["CÓDIGO DO PLANO"],
                    "nome_plano": ["DESCRIÇÃO DO PLANO"],
                    "logradouro": ["ENDEREÇO"],
                    "bairro": ["BAIRRO"],
                    "cidade": ["CIDADE"],
                    "estado": ["UF"],
                    "cep": ["CEP"],
                    "lotacao": ["CÓDIGO DA EMPRESA"],
                    "inclusao_plano_real": ["DATA DE INCLUSÃO"]
                  }
                },
                "cad_v1": {
                  "initial_columns": [
                    "OPERADORA", "APOLICE", "CODIGOXEMPRESA", "NOMEXEMPRESA", "CODIGOXLOCAL", "DESCRICAOXLOCAL", "MATRICULA", "CARTÃO", "NOME", "CPF", "CARGO", "DATAXNASCIMENTO", "SEXO", "ELEGIBILIDADE", "CODIGOXGRAU", "DESCRICAOXGRAU", "ESTADOXCIVIL", "CODIGOXPLANO", "DESCRICAOXPLANO", "ENDERECO", "CIDADE", "UF", "DATAXCADASTRO", "DATAXADESAO", "DATAXCANCELAMENTO", "CARTEIRINHATITULAR"
                  ],
                  "columns": {
                    "nome": ["NOME"],
                    "cpf": ["CARTÃO"],
                    "sexo": ["SEXO"],
                    "bkp": ["MATRICULA"],
                    "matricula_familia": ["CARTEIRINHATITULAR"],
                    "cod_plano": ["CODIGOXPLANO"],
                    "nome_plano": ["DESCRICAOXPLANO"],
                    "nascimento": ["DATAXNASCIMENTO"],
                    "titularidade": ["DESCRICAOXGRAU"],
                    "inclusao_plano_real": ["DATAXADESAO"],
                    "cancelamento_plano_real": ["DATAXCANCELAMENTO"],
                    "subestipulante": ["NOMEXEMPRESA"],
                    "lotacao":["CODIGOXEMPRESA"]
                  }
                },
                "ev_v1": {
                  "initial_columns": [
                    "OPERADORA", "EMPRESA", "NOMEXEMPRESAXPRINICPAL", "CONTRATO", "NOMEXEMPRESA", "CODXLOCAL", "DESCXLOCAL", "MATRICULA", "CARTAXTITULAR", "NOMEXTITULAR", "CPF", "CARGO", "CARTÃO", "NOME", "DATAXNASCIMENTO", "SEXO", "ELEGIBILIDADE", "CODIGOXGRAU", "DESCRICAOXGRAU", "ESTADOXCIVIL", "CODIGOXPLANO", "DESCRICAOXPLANO", "STATUS", "CIDADE", "UF", "CODIGOXSERVICO", "DESCRICAOXSERVICO", "TABELA", "CODIGOXGRUPO", "DESCRICAOXGRUPO", "CODIGOXSUBGRUPO", "DESCRICAOXSUBGRUPO", "CODIGO ESPECIALIDADE", "DESCRICAXESPECIALIDADE", "CODIGOXCID", "DESCRICAOXCID", "DOCUMENTO", "SENHA", "TIPOXUTILIZACAO", "DATA REALIZACAO", "DATA ALTA", "COMPETENCIA", "CODIGOXPRESTADOR", "NOMEXPRESTADOR", "QTDXSINISTRO", "VALORXAPRESENTADO", "VALORXPAGO", "VALORXEMPRESA", "VALORXUSUARIO", "TIPO CONSELHO", "UF CONSELHO", "NUM. CONSELHO", "TIPO PRESTADOR", "QTDXPAGA", "IND_USO", "a1", "a2", "a3", "a4", "a5", "a6"
                  ],
                  "columns": {
                    "nome": ["NOME"],
                    "cpf": ["CPF"],
                    "cpf_real": ["CARTÃO"],
                    "matricula": ["MATRICULA"],
                    "cod_plano": ["CODIGOXPLANO"],
                    "internacao": ["CODIGOXGRUPO"],
                    "subcategoria": ["CODIGOXSUBGRUPO"],
                    "nome_plano": ["DESCRICAOXPLANO"],
                    "nascimento": ["DATAXNASCIMENTO"],
                    "titularidade": ["DESCRICAOXGRAU"],
                    "inclusao_plano": ["DATAXADESAO"],
                    "cancelamento_plano": ["DATAXCANCELAMENTO"],
                    "subestipulante": ["NOMEXEMPRESA"],
                    "lotacao":["CONTRATO"]
                  }
                },
                "norm_v1": {
                  "initial_columns": [
                    "TP REGISTRO", "COD.EMPRESA", "MAT. FUNCIONAL", "CONT. BEN.", "TP BENF", "MARCA OTICA DEP.", "MARCA OT. TITULAR", "NOME BENEFICIÁRIO", "COD. PRESTADOR", "NOME PRESTADOR", "NOME PROCEDIMENTO", "VL PROCEDIMENTO", "TP PROCED", "DATA DO PROCEDIMENTO", "ESPECIALIDADE", "DATA DO CANCELAMENOT", "COMPETENCIA", "NOME TITULAR", "COD PROCEDIMENTO", "DOC", "NUM. DO DOCUMENTO"
                  ],
                  "columns": {
                    "nome": ["NOME BENEFICIÁRIO"],
                    "cpf": ["CPF"],
                    "cpf_real": ["CARTÃO"],
                    "matricula": ["MATRICULA"],
                    "cod_plano": ["CODIGOXPLANO"],
                    "nome_plano": ["DESCRICAOXPLANO"],
                    "nascimento": ["DATAXNASCIMENTO"],
                    "titularidade": ["DESCRICAOXGRAU"],
                    "inclusao_plano": ["DATAXADESAO"],
                    "cancelamento_plano": ["DATAXCANCELAMENTO"],
                    "subestipulante": ["NOMEXEMPRESA"]
                  }
               }
            },
            "etl": {
                "v1": [
                    {"function": ben_v1_titular_encadeado},
                    # {"function": parse_date, "params": {"formato": "%d/%m/%Y"}},
                    # {"function": parse_cpf_from_db, "params": {"force_md5": True}},
                    {"function": parse_titularidade},
                    {"function": ben_fix_inclusao_cancelamento},
                    {"function": parse_competencia},
                    {"function": inferir_status},
                    {"function": ben_v1_custom_nascimento},
                    {"function": define_ONE_NEXT_or_Amil},
                    {"function": Aplicar_DEV_Filtros},
                    {"function":lower_names_filters},
                    {"function":cancelar_futuro},

                ],
                "benef_v1": [
                    {"function": parse_nome_upper},
                    {"function": ben_v2_titular_encadeado},
                    {"function": parse_date, "params": {"formato": "%d/%m/%Y"}},
                    {"function": parse_competencia},
                    {"function": ben_fix_inclusao_cancelamento},
                    # {"function": parse_cpf_from_db, "params": {"force_md5": True}},
                    {"function": parse_sexo},
                    {"function": parse_titularidade},
                    {"function": inferir_status},
                    {"function": define_ONE_NEXT_or_Amil},
                    {"function": Aplicar_DEV_Filtros},
                    {"function":lower_names_filters},
                    {"function":cancelar_futuro},
                ],
                "cad_v1": [
                    {"function": parse_competencia},
                    {"function": ben_cad_v1_parse_date},
                    {"function": parse_sexo},
                    {"function": applies, 
                      "params": {
                        "columns": ["inclusao_plano_real", "cancelamento_plano_real"],
                        "destinations" :  ["inclusao_plano", "cancelamento_plano"]}
                    },
                    {"function": ben_fix_inclusao_cancelamento},
                    {"function": inferir_status},
                    {"function": parse_titularidade},
                    {"function": ben_cad_v1_parse_titular},
                    {"function": set_matricula_familia},
                    {"function": parse_subestipulante},
                    {"function": parse_cpf_from_db, "params": {"force_md5": True}},
                    {"function": define_ONE_NEXT_or_Amil},
                    {"function": Aplicar_DEV_Filtros},
                    {"function":lower_names_filters},
                    {"function":cancelar_futuro},   
                    {"function": definindo_cpf_real},
                    {"function":transpose_titular_mensalidade}
                ]
            }
        },
        "ev": {
            "read_params": {
                "v1": {
                  "initial_columns": [
                    "OPERADORA", "EMPRESA", "NOMEXEMPRESAXPRINICPAL", "CONTRATO", "NOMEXEMPRESA", "CODXLOCAL", "DESCXLOCAL", "MATRICULA", "CARTAXTITULAR", "NOMEXTITULAR", "CPF", "CARGO", "CARTÃO", "NOME", "DATAXNASCIMENTO", "SEXO", "ELEGIBILIDADE", "CODIGOXGRAU", "DESCRICAOXGRAU", "ESTADOXCIVIL", "CODIGOXPLANO", "DESCRICAOXPLANO", "STATUS", "CIDADE", "UF", "CODIGOXSERVICO", "DESCRICAOXSERVICO", "TABELA", "CODIGOXGRUPO", "DESCRICAOXGRUPO", "CODIGOXSUBGRUPO", "DESCRICAOXSUBGRUPO", "CODIGO ESPECIALIDADE", "DESCRICAXESPECIALIDADE", "CODIGOXCID", "DESCRICAOXCID", "DOCUMENTO", "SENHA", "TIPOXUTILIZACAO", "DATA REALIZACAO", "DATA ALTA", "COMPETENCIA", "CODIGOXPRESTADOR", "NOMEXPRESTADOR", "QTDXSINISTRO", "VALORXAPRESENTADO", "VALORXPAGO", "VALORXEMPRESA", "VALORXUSUARIO", "TIPO CONSELHO", "UF CONSELHO", "NUM. CONSELHO", "TIPO PRESTADOR", "a", "b", "c", "d", "e", "f", "g"],
                  "columns": {
                    "subestipulante": ["NOMEXEMPRESA"],
                    "cnpj_subestipulante": ["CONTRATO"],
                    "cpf_titular": ["CARTAXTITULAR"],
                    "nome_titular": ["NOMEXTITULAR"],
                    "cpf": ["CARTÃO"],
                    "matricula_familia": ["MATRICULA"],
                    "nome_beneficiario": ["NOME"],
                    "nascimento" : ["DATAXNASCIMENTO"],
                    "sexo": ["SEXO"],
                    "cod_plano": ["CODIGOXPLANO"],
                    "plano": ["DESCRICAOXPLANO"],
                    "status": ["STATUS"],
                    "codigo_procedimento": ["CODIGOXSERVICO"],
                    "descricao":["DESCRICAOXSERVICO"],
                    "internacao": ["CODIGOXGRUPO"],
                    "subcategoria": ["CODIGOXSUBGRUPO"],
                    "especialidade": ["DESCRICAXESPECIALIDADE"],
                    "cod_cid": ["CODIGOXCID"],
                    "titularidade": ["DESCRICAOXGRAU"],
                    "desc_cid": ["DESCRICAOXCID"],
                    "guia": ["DOCUMENTO"],
                    "tipo_sinistro": ["TIPOXUTILIZACAO"],
                    "data_inicio": ["DATA REALIZACAO"],
                    "competencia": ["COMPETENCIA"],
                    "cod_executor": ["CODIGOXPRESTADOR"],
                    "executor": ["NOMEXPRESTADOR"],
                    "valor": ["VALORXPAGO"],
                    "quantidade":["QTDXSINISTRO"],
                  }
               },
               "v2": {
                   "columns":{
                   "competencia":["COMPETENCIA BOOK"],
                   "nome_beneficiario":["NOME DO BENEFICIARIO"],
                   "titularidade":["TIPO BENEFICIARIO"],
                   "parentesco":["GRAU PARENTESCO"],
                   "nascimento":["DATA NASCIMENTO"],
                   "cod_associado":["NUMERO DO BENEFICIARIO"],
                   "matricula_familia":["MATRICULA DO BENEFICIARIO"],
                   "plano":["NOME PLANO"],
                   "cod_plano":["CODIGO PLANO"],
                   "codigo_procedimento":["PROCEDIMENTO"],
                   "descricao":["DESCRICAO PROCEDIMENTO"],
                   "especialidade":["ESPECIALIDADE"],
                   "data":["DATA DE ATENDIMENTO"],
                   "data_pagamento":["COMPETENCIA PAGTO"],
                   "executor":["NOME DO PRESTADOR"],
                   "tipo_sinistro":["TIPO ATENDIMENTO"],
                   "valor":["VALOR DO PROCEDIMENTO"],
                   "cidade":["CIDADE BENEFICIARIO"],
                   "estado":["UF BENEFICIARIO"],
                   "bkp":["CODIGO EMPRESA"],
                   "bkp2":["NOME EMPRESA"]
                   }
               }
            },
            "etl": {
                "v1": [
                  {"function": parse_sexo},
                  {"function": parse_float},
                  {"function": ev_v1_parse_titularidade},
                  {"function": parse_subestipulante},
                  {"function": parse_titularidade},
                  {"function": ev_v1_parse_categoria},
                  {"function": parse_competencia},
                  {"function": parse_date,
                               "params": {
                                   "formato": "%d/%m/%Y"
                               }
                  },
                  {"function": parse_cpf_from_db, "params": {"force_md5": True}},
                  {"function": remove_tracos_e_pontos},
                  {"function": ev_v1_parse_replace_status},
                  {"function": clear_zeros},
                  {"function": define_ONE_NEXT_or_Amil},
                  {"function": Aplicar_DEV_Filtros},
                  {"function": pre_tabela_mae},
                  {"function": Aplicar_Tabela_Mae, "params":{"segmentar_exames": False} },
                  {"function": pos_tabela_mae},
                  {"function":lower_names_filters},
                  {"function": ev_v1_parse_peona},
              ],
              "v2":[
                  {"function": define_ONE_NEXT_or_Amil},
                  {"function": lower_names_filters},
                  {"function": fix_data_dental},
                  {"function": fix_valor_dental},
                  {"function": parse_cpf_from_db, "params": {"force_md5": True}},
                  {"function": definindo_titular_dental},
                  {"function": concatenar_subestipulante}

                  
                  ]
            },
        },
        "men": {
            "read_params": {
                "v1": {
                  "columns": {
                    "cpf": ["Código"],
                    "nome": ["Beneficiário"],
                    "matricula_familia": ["Matrícula"],
                    "plano": ["Plano"],
                    "titularidade": ["Dependência"],
                    "mensalidade": ["Mensalidade"],
                    "coparticipacao": ["Co-Participacao"],
                    "outros": ["Outros"],
                    "bkp": ["Rubrica"],
                    #"data_limite": ["Data Limite"],
                    "data_inclusao": ["Data Inclusão"],
                    "data_exclusao": ["Data Exclusão"],
                  }
                }
            },
            "etl": {
                "v1": [
                    {"function": parse_competencia, "params": { "tabela": "BI_Mensalidades"}},
                    {"function": parse_date},
                    {"function": men_v1_clear_df},
                    {"function": remove_tracos_e_pontos},
                    {"function": save_columns},
                    {"function": parse_float},
                    {"function": parse_titularidade, 
                     "params": { 
                         "null_eh_titular": True
                        } 
                    },
                    {"function": ben_cad_v1_parse_titular},
                    {"function": parse_cpf_from_db, "params": {"force_md5": True}},
                    {"function": men_v1_parse_subestipulante_cyrela, "company_only": ["170"]},
                    {"function": drop_columns, 
                     "params": {
                         "columns": ["matricula_familia"]
                     } 
                    },
                    {"function": men_v1_parse_datas_extras_e_status},
                    {"function": define_ONE_NEXT_or_Amil},
                    {"function": Aplicar_DEV_Filtros},
                    {"function": transpose_titular_mensalidade},
                    {"function": transpose_sexo_mensalidade},
                    {"function":lower_names_filters},
                ]
            }
        },
        "cop": {
            "read_params": {
                "v1": {
                    "initial_columns": [
                        "BKP", "CONTRATO", "MATRICULA", "f", "DESCRICAOXGRAU", "CARTÃO", "CARTAXTITULAR", "NOME", "a", "b", "c", "VALORXPAGO", "DESCRICAOXPLANO", "DATA REALIZACAO", "d", "e", "COMPETENCIA", "CODIGOXPLANO", "BKP3"],
                    "columns": {
                        "bkp": ["BKP"],
                        "cnpj_subestipulante": ["CONTRATO"],
                        "matricula_familia": ["MATRICULA"],
                        "titularidade": ["DESCRICAOXGRAU"],
                        "cpf_real": ["CARTÃO"],
                        "cpf_real_titular": ["CARTAXTITULAR"],
                        "nome_beneficiario": ["NOME"],
                        "coparticipacao": ["VALORXPAGO"],
                        "estado": ["DESCRICAOXPLANO"],
                        "data_inicio": ["DATA REALIZACAO"],
                        "competencia": ["COMPETENCIA"],
                        "bkp3": ["BKP3"],
                        "parentesco": ["CODIGOXPLANO"]
                    }
                }
            },
            "etl": {
                "v1": [
                    {"function": parse_float},
                    {"function": parse_subestipulante},
                    {"function": parse_titularidade},
                    {"function": parse_date_from_date_identifier},
                    # {"function": parse_date},
                    # {"function": clear_zeros},
                    {"function": parse_cpf_from_db, "params": { "force_md5": True }},
                    {"function": remove_tracos_e_pontos},
                    {"function": define_ONE_NEXT_or_Amil},
                    {"function": Aplicar_DEV_Filtros},
                    {"function":lower_names_filters},
                ]
            }
        }
    }