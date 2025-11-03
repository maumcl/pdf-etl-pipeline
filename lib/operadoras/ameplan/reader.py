import tabula
import pandas as pd
from lib.legado.legado import *
from lib.df_functions import *

def read_rg(file_path, **kwargs):
    df = pd.DataFrame()
    version = None
    if "sinistralidade" in file_path.lower():
        version = "v1"
        column_dict = kwargs.get(version)["columns"]
        df = tabula.read_pdf(file_path, pages="1", area=[150,10,750,1910], encoding='latin1', pandas_options={"dtype": "str"})
        df = pd.concat(df)
        df.columns = range(df.shape[1])
        df = df[~df[0].isnull()]
        df = df[df[0]!='Total']
        df = columns(df, column_dict=column_dict)
        display(df)
    return df, version

def read_ben(file_path, **kwargs):
    if ".xls" in file_path.lower():
        print("script selecionado: v1")
        version = "v1"
        df = pd.read_excel(file_path, dtype=str)
        df = columns(df, column_dict=kwargs.get("v1")["columns"])

    return df, version

def read_ev(file_path, **kwargs):
    if ".xls" in file_path.lower():
        print("script selecionado: v1 (operadora/extensao)")
        version = "v1"
        df = pd.read_excel(file_path, dtype=str)
        df = columns(df, column_dict=kwargs.get("v1")["columns"])

    return df, version