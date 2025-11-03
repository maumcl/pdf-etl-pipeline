import json
import tabula
import pandas as pd
from lib.legado.legado import *
from lib.df_functions import *
from .specifics import *
from .specifics import corr_oswaldo_cruz

        
def read_ev(file_path, **kwargs):

    if "amil dental" in file_path.lower():
        print("Script Selcionado: v2 (Amil dental)")
        version = "v2"
        df = pd.read_csv(file_path,sep=";",dtype=str,encoding='ISO-8859-1')
        df = columns(df, column_dict=kwargs.get("v2")["columns"])
        


    else:
        print("Script selecionado:V1")
        version = "v1"
        df = pd.read_csv(file_path, header=None, sep="\t", skiprows=0, dtype=str)
        df.columns = (kwargs.get("v1")["initial_columns"] + list(range(10)))[:len(df.columns)]
        df = columns(df, column_dict=kwargs.get("v1")["columns"])
        df = clear_df(df)

    return df, version
        
def read_rg(file_path, **kwargs):
    if ".pdf" in file_path.lower():
        print("Script selecionado: v2 (pdf)")
        version = "v2"
        df = tabula.read_pdf(file_path, pages='1', stream=False, encoding='utf-8')
        df = pd.DataFrame(df[0], dtype='str')
        df = columns(df, column_dict=kwargs.get("v2")["columns"])
        df = df[df['data'].notna()]
        df = df[df['data'] != 'nan']
        df = df[df['data'] != 'Total']
    else:
        ## SUBIR RG SEPARADO POR SUBESTIPULANTE
        if "porempresa" in file_path.lower():
            print("Script selecionado: v3 (separado por subestipulante)")
            version = "v3"
            df = []
            for row in range(0, 20):
                df = pd.read_excel(file_path, skiprows=row, dtype=str)
                if "Data de Competência" in df.columns:
                    df.dropna(subset=["Data de Competência"], inplace=True)
                    df = df[df["Data de Competência"] != "Total"]
                    for column in df.columns:
                        if "unnamed" in column.lower():
                            df.drop(columns=[column], inplace=True)
                    break
            df = columns(df, column_dict=kwargs.get("v3")["columns"])

            # FUNCAO P PREENCHER SUBESTIPULANTE CONFORME A PRIMEIRA LINHA
            df["subestipulante"]= df["subestipulante"].fillna(method='ffill')

            # PREENCHER COM VALOR='0' QUANDO NULL
            df['vidas'] = df['vidas'].fillna(0)
            df['sinistralidade'] = df['sinistralidade'].fillna(0)
            df['sinistro'] = df['sinistro'].fillna(0)
            df['faturamento'] = df['faturamento'].fillna(0)

            df = clear_df(df)

        else:
            print("Script selecionado: v1 (default)")
            df = []
            workbook = pd.ExcelFile(file_path)
            sheetname = workbook.sheet_names
            if "SINISTRALIDADE INDIVIDUAL" in sheetname:
                version = "v4"
                for row in range(0, 20):
                    df = pd.read_excel(file_path, skiprows=row, dtype=str, sheet_name='SINISTRALIDADE INDIVIDUAL')
                    if "MÊS" in df.columns:
                        df.dropna(subset=["MÊS"], inplace=True)
                        df = df[df["MÊS"] != "TOTAL GERAL"]
                        for column in df.columns:
                            if "unnamed" in column.lower():
                                df.drop(columns=[column], inplace=True)
                        break
                df = columns(df, column_dict=kwargs.get("v4")["columns"])
                df = clear_df(df)
            else:
                version = "v1"
                for row in range(0, 20):
                    df = pd.read_excel(file_path, skiprows=row, dtype=str)
                    if "Data de Competência" in df.columns:
                        df.dropna(subset=["Data de Competência"], inplace=True)
                        df = df[df["Data de Competência"] != "Total"]
                        for column in df.columns:
                            if "unnamed" in column.lower():
                                df.drop(columns=[column], inplace=True)
                        break
                df = columns(df, column_dict=kwargs.get("v1")["columns"])
                df = clear_df(df)
        return df, version

def read_ben(file_path, **kwargs):
    if "demonst" in file_path.lower():
        raise NotImplementedError
    elif "__c_" in file_path.lower():
        print("Script selecionado: cad_v1")
        version = "cad_v1"
        df = pd.read_csv(file_path, header=None, skiprows=0, sep="\t", encoding="Windows-1254")
        df.columns = kwargs.get("cad_v1")["initial_columns"][:len(df.columns)]
        df = columns(df, column_dict=kwargs.get("cad_v1")["columns"])
    elif "benef_v1" in file_path.lower():
        print("Script selecionado: benef_v1")
        version = "benef_v1"
        df = pd.read_csv(file_path, skiprows=0, sep="^", encoding="Windows-1254")
        df = columns(df, column_dict=kwargs.get("benef_v1")["columns"])

    elif "oswaldo" in file_path.lower() and ".xlsx" in file_path.lower():
        version = 'v1'
        print('Script selecionado: v1 (default)')
        df = pd.read_excel(file_path, header=5)

        df = corr_oswaldo_cruz(df)

        df = columns(df, column_dict=kwargs.get("v1")["columns"])

    else:
        print("Script selecionado: v1 (default)")
        version = "v1"
        df = pd.read_csv(file_path, sep="#", encoding="Windows-1254", skiprows=5)
        df = df[df["Código"] != "Código"]
        with open(file_path, encoding="Windows-1254") as file:
            lines = file.readlines()
        line = lines[2]
        data = line.split("Vencimento: ")[-1].split("#")[0]
        df["competencia"] = "01/" + data[3:]
        df["subestipulante"] = line.split("Contrato: ")[-1].split("#")[0]
        df["subestipulante"] = df["subestipulante"].apply(lambda x: x.split("-")[-1].strip())
        df = columns(df, column_dict=kwargs.get("v1")["columns"])
    df = df[df["nome"].notnull()]
    df = clear_df(df)
    return df, version

def read_men(file_path, **kwargs):
    df = pd.read_csv(file_path, sep="#", encoding="Windows-1254", skiprows=5, dtype=str)
    df = columns(df, column_dict=kwargs.get("v1")["columns"])
    df = clear_df(df)

    #BKP Apolice
    first_contrato = pd.read_csv(file_path, sep="#", encoding="Windows-1254", skiprows=2, nrows=1, header=None, dtype=str).iloc[0,0].split(":")[-1].strip()
    df = men_v1_set_sub_bkp(df, first_contrato, file_path)
    df['subestipulante'] = None #Por enquanto o DEV_Filtros não cria a coluna automaticamente...
    
    version = "v1"
    df = df[df["cpf"].notna()]
    return df, version

def read_cop(file_path, **kwargs):
    df = pd.read_csv(file_path, header=None, sep=";", skiprows=1, dtype=str)
    df.columns = (kwargs.get("v1")["initial_columns"] + list(range(10)))[:len(df.columns)]
    df = columns(df, column_dict=kwargs.get("v1")["columns"])
    df = clear_df(df)
    return df, "v1"

    
