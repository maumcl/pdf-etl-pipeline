import pandas as pd
import numpy as np
import re
import unidecode
import lib.df_functions as df_functions
from datetime import datetime

""" FUNÇÕES LEGADAS DE WELLBEFUNCTIONS, COM ALGUMAS MODIFICAÇÕES """

def columns(df, *args, **kwargs):
    """
    Função para mapear colunas do dataframe.
    Args:
        df: pandas.DataFrame
        column_dict: dict, default {}
            Dicionário o qual as chaves serão usadas para manter as colunas e os valores
            para renomeá-las.
        invert: bool, default True
            Se deve ser utilizado o formato de valor -> lista de chaves,
            onde as listas de chave serão os campos buscados na coluna.
    Returns:
        pandas.DataFrame
           Dataframe com as colunas referentes aos valores do dicionário.
    """
    column_dict = kwargs.get("column_dict", {})
    invert = kwargs.get("invert", True)
    
    #Verificando se existem colunas com mesmo nome e criando o sulfixo para cada uma
    if not df.loc[:,df.columns.duplicated()].empty:
        print("---\n\x1b[38;5;3m{}\x1b[0m".format("Existem colunas duplicadas. Verifique o dict de tradução das seguintes colunas:"))
        print(", ".join(df.loc[:,df.columns.duplicated()].columns.tolist()))

        #Renomeando colunas para col_1, col_2 etc...
        col_names = df.columns.to_list()
        col_duplicateds = df.loc[:,df.columns.duplicated()].columns.to_list()

        for duplicated in col_duplicateds:
            counter = 0
            for i in range(len(col_names)):
                if col_names[i] == duplicated:
                    #print(f"Duplicado {col_names[i]}")
                    col_names[i] = duplicated+f"_{counter}"
                    counter +=1
        df.columns = col_names
        
    #Salvando BKP das linhas
    df['row_data'] = df.to_json(orient='records', lines=True, force_ascii=False).splitlines()

    df = df.rename(columns=lambda column: re.sub(' +', ' ', ' '.join(str(column).strip().split())))

    if invert:
        column_dict_inverted = {}
        for k, v in column_dict.items():
            for vv in v:
                column_dict_inverted[vv] = k
    else:
        column_dict_inverted = column_dict

    cols_to_stay = list(column_dict_inverted)
    cols_to_stay.append('row_data')

    df = df.drop(columns=df.columns.difference(list(cols_to_stay)), axis=1)
    #column_dict_inverted = {k: v for k, v in column_dict_inverted.items() if k in df.columns}
    
    df = df.rename(columns=column_dict_inverted)
    df = linecode(df)
    return df


def linecode(df):
    df["lcode"] = df.index + 1
    return df


def categorical(df, *args, **kwargs):
    """
    Função que mapeia valores de determinada coluna de acordo com um
    dicionário, esse podendo ser str: str ou str: List[str].
    Muito útil para mapear titularidades, sexo ou qualquer outro campo
    categórico.
    Args:
        df: pandas.DataFrame
        column: str
            Coluna que será origem do valor.
        destination: str, default `columns`
            Coluna que será destino do valor.
        column_dict: Union[dict[str, str], dict[str, list[str]]]
            Coluna que relaciona chave -> valor ou valor -> lista de chaves
        default: Any
            Valor padrão caso não exista no dicionário.
        value_as_default: bool, default True
            Utilizar o próprio valor como padrão caso não seja encontrado
            na lista de chaves
        invert: bool, default True
            Se deve ser utilizado o formato de valor -> lista de chaves,
            onde as listas de chave serão os campos buscados na coluna.
        f: Função, default id
            Função para tratar valor antes de buscar no dicionário.
    Returns:
        pandas.DataFrame
           Dataframe com apply aplicado.
    """
    column = kwargs.get("column", None)
    destination = kwargs.get("destination", column)
    column_dict = kwargs.get("column_dict", {})
    default = kwargs.get("default", None)
    value_as_default = kwargs.get("value_as_default", False)
    coalesce = kwargs.get("coalesce", True)
    invert = kwargs.get("invert", True)
    f = kwargs.get("f", lambda x: x)
    #print("legado.categorical: destination ->",destination)

    assert column is not None, "coluna não informada."

    if invert:
        column_dict_inverted = {}
        for k, v in column_dict.items():
            for vv in v:
                column_dict_inverted[vv] = k
    else:
        column_dict_inverted = column_dict

    old = None
    if destination in df:
        old = df[destination].copy()
    df[destination] = df[column].apply(
        lambda x: column_dict_inverted.get(f(x), default if not value_as_default else x)
    )

    df[destination] = df[destination].astype(str)
    df[destination] = df[destination].apply(
        lambda s: s if str(s) not in [None, "nan", "None", "", "none"] else None
    )
    if coalesce and old is not None:
        df[destination] = df[destination].combine_first(old)
    return df

def clear_df(df, **kwargs):
    """
    Função para limpar o dataframe: 
        Remove espacos antes e depois de uma string/object
        Remove os caracteres ", =, 
        Substitui o caracter / por . de plano e subestipulante
    Aplicado em todas colunas do tipo string/object
    Args:
        df: pd.Dataframe

    Returns:
        o mesmo dataframe com as colunas tratadas
    """
    def clear_barras(data):
        return data if pd.isnull(data) else str(data).replace("/", ".")

    if "plano" in df.columns:
        df["plano"] = df["plano"].apply(clear_barras)
    if "nome_plano" in df.columns:
        df["nome_plano"] = df["nome_plano"].apply(clear_barras)
    if "subestipulante" in df.columns:
        df["subestipulante"] = df["subestipulante"].apply(clear_barras)
    if "titularidade" in df.columns:
        df["titularidade"] = df["titularidade"].apply(clear_barras)
    if "parentesco" in df.columns:
        df["parentesco"] = df["parentesco"].apply(clear_barras)
    if "dependencia" in df.columns:
        df["dependencia"] = df["dependencia"].apply(clear_barras)
    if "sexo" in df.columns:
        df["sexo"] = df["sexo"].apply(lambda x: np.nan if str(x) in ["-1"] else x)

   
    for column in df.columns:
        if column != 'row_data':
            df[column] = df[column].apply(lambda x: x.replace('"', "")
                .replace("=", "")
                .replace(u"\u00A0", "")
                .replace("R$", "")
                .replace("\'", "") 
                if isinstance(x, str) else x)
            
            df[column] = df[column].apply(lambda x: x.strip() if isinstance(x, str) else x)
    return df

def set_carimbo(df, carimbo=None, db=None, descricao='', script_version=0, script_name=''):
    """
    Função para criar/setar o valor do carimbo
    Args:
        df: pd.DataFrame.
        carimbo: int, valor do carimbo para forçar.
        db: WellbeDatabase, instância do banco.
        descricao: str, descrição do carimbo.
        script_version: int, versão do script.
        script_name: str, nome do script.

    Returns:
        pandas.DataFrame
            Dataframe com a coluna atribuída.

    """

    if carimbo is not None:
        df["carimbo"] = carimbo
        return df

    empresa_id = str(df["empresa_id"].value_counts().idxmax())
    operadora = str(df["operadora"].value_counts().idxmax())
    file_path = str(df["fcode"].value_counts().idxmax())

    carimbo = db.generateStamp(empresa_id, file_path, descricao, operadora=operadora, script_version=script_version, script_name=script_name)

    df["carimbo"] = carimbo
    return df

def debug(df, *args, **kwargs):
    """
    Função para printar/debugar dataframe atual.
    Args:
        df: pandas.DataFrame
        rows: int, default 1
            Quantidade de linhas que serão exibidas.
    Returns:
        pandas.DataFrame
            Mesmo dataframe do parâmetro df.
    """
    from IPython.display import display
    rows = kwargs.get("rows", 1)
    display(df.head(rows))
    return df


def like_categorical(df, *args, **kwargs):
    """
    Função para mapear dicionários através do like das chave.

    Args:
        df: pandas.DataFrame
        column_dict: dict, default {}
           Dicionário chave -> valor utilizado para mapear coluna.
        invert: bool, default True
            Se deve ser utilizado o formato de valor -> lista de chaves,
            onde as listas de chave serão os campos buscados na coluna.
        use_max_freq: bool, default False
            Se deve ser utilizada o valor com mais ocorrências em todas
            as linhas dessa coluna. Útil para tratar subestipulantes.

    Returns:
        pandas.DataFrame
           Dataframe com a coluna referente mapeada.
    """
    column = kwargs.get("column", None)
    destination = kwargs.get("destination", column)
    column_dict = kwargs.get("column_dict", {})
    invert = kwargs.get("invert", True)
    use_max_freq = kwargs.get("use_max_freq", False)
    ignore = kwargs.get("ignore", True)
    show_logs = kwargs.get("show_logs", True)

    assert column is not None, "coluna não informada."
    if column not in df:
        return df


    if invert:
        column_dict_inverted = {}
        for k, v in column_dict.items():
            for vv in v:
                column_dict_inverted[vv] = k
    else:
        column_dict_inverted = column_dict

    def inner(x):
        aux = [
            y
            for y in column_dict_inverted.keys()
            if y.lower() in unidecode.unidecode(str(x)).lower()
        ]
        if len(aux) > 0:
            return column_dict_inverted.get(aux[0], x)
            
        if use_max_freq:
            return None

        if ignore:
            if show_logs:
                print(f"Valor `{x}` não existe no dicionário")
            return x
        raise ValueError(f"Coluna: `{column}`, valor: `{x}` não existe no dicionário")

    df[destination] = df[column].apply(inner)
    if use_max_freq:
        max_freq_value = df[~pd.isnull(df[destination])][destination].value_counts().idxmax()
        df[destination] = max_freq_value
    return df

def set_carimbo(df, carimbo=None, db=None, descricao='', script_version=0, script_name=''):
    """
    Função para criar/setar o valor do carimbo
    Args:
        df: pd.DataFrame.
        carimbo: int, valor do carimbo para forçar.
        db: WellbeDatabase, instância do banco.
        descricao: str, descrição do carimbo.
        script_version: int, versão do script.
        script_name: str, nome do script.

    Returns:
        pandas.DataFrame
            Dataframe com a coluna atribuída.

    """
    if carimbo is not None:
        df["carimbo"] = carimbo
        return df

    empresa_id = str(df["empresa_id"].value_counts().idxmax())
    operadora = str(df["operadora"].value_counts().idxmax())
    file_path = str(df["fcode"].value_counts().idxmax())

    carimbo = db.generateStamp(empresa_id, file_path, descricao, operadora=operadora, script_version=script_version, script_name=script_name)

    df["carimbo"] = carimbo

    return df

def parse_cpf_from_db(df, db=None, force_md5=False):
    """
    Função que aplica md5 do nome no cpf de acordo com os registros do banco

    Args:
        df: pandas.Dataframe
        db: WellbeDatabase
            Instância de db

    Returns:
        pandas.DataFrame
            Dataframe original
    """
    if db is None:
        print("Lib: parse_cpf_from_db: instância do banco não informada")
        return df

    if "operadora" not in df:
        print("Lib: parse_cpf_from_db: faltando operadora no dataframe")
        return df

    if "empresa_id" not in df:
        print("Lib: parse_cpf_from_db: faltando empresa_id no dataframe")
        return df

    if force_md5:
        print("Lib: parse_cpf_from_db: md5 aplicado, flag force_md5")
        return df_functions.new_md5(df)

    operadora = df["operadora"].value_counts().idxmax()
    empresa_id = df["empresa_id"].value_counts().idxmax()

    try:
        query = """
            select length(max(cpf)) l 
            from {table}
            where operadora = '{operadora}'
              and Empresa_ID = '{empresa_id}'
        """.format(
                    column="cpf",
                    operadora=operadora,
                    empresa_id=empresa_id,
                    table="BI_Beneficiario",
                )
        l = db.runQueryWithPandas(query)
        l = l['l'][0]

    except Exception as e:
        print(
            "Erro ao conectar no banco, talvez a coluna não exista nessa tabela")
        print(e)
        return df
    
    if l is None and not force_md5:
        print("Lib: parse_cpf_from_db: md5 não aplicado, não há dados no banco para referência.")
        return df

    if l == 32:
        print("Lib: parse_cpf_from_db: md5 aplicado, seguindo padrão do banco")
        return df_functions.new_md5(df)

    print("Lib: parse_cpf_from_db: md5 não aplicado, seguindo padrão do banco")
    return df

def parse_mtm_from_db(df, db=None, force_mtm=False, threshold=0.25):
    """
    Função que aplica mes a mes de acordo com os registros do banco

    Args:
        df: pandas.Dataframe
        db: WellbeDatabase
            Instância de db
        force_mtm: Bool, flag para forçar mês a mês
        threshold: Float, proporção de pessoas mês a mês 

    Returns:
        pandas.DataFrame
            Dataframe original
    """
    if db is None:
        print("Lib: parse_mtm_from_db: instância do banco não informada")
        return df

    if "operadora" not in df:
        print("Lib: parse_mtm_from_db: faltando operadora no dataframe")
        return df

    if "empresa_id" not in df:
        print("Lib: parse_mtm_from_db: faltando empresa_id no dataframe")
        return df
    
    if "competencia" not in df:
        print("Lib: parse_mtm_from_db: faltando competencia no dataframe")
        return df

    operadora = df["operadora"].value_counts().idxmax()
    empresa_id = df["empresa_id"].value_counts().idxmax()

    try:
        query = """
        select sum(if(t1.data_dif = 1, pessoas, 0)) / sum(pessoas) as pessoas_mtm_prop
        from (select count(distinct cpf)                                         pessoas,
                     TIMESTAMPDIFF(MONTH, inclusao_plano, DATE_ADD(cancelamento_plano, INTERVAL 3 DAY)) as data_dif
            from BI_Colaborador
            where operadora = '{operadora}'
                and Empresa_ID = '{empresa_id}'
                and inclusao_plano is not null
                and cancelamento_plano is not null
                and inclusao_plano <> cancelamento_plano
            group by 2
            union all
            select count(distinct cpf)                                         pessoas,
                   TIMESTAMPDIFF(MONTH, inclusao_plano, DATE_ADD(cancelamento_plano, INTERVAL 3 DAY)) as data_dif
            from BI_Beneficiario
            where operadora = '{operadora}'
                and Empresa_ID = '{empresa_id}'
                and inclusao_plano is not null
                and cancelamento_plano is not null
                and inclusao_plano <> cancelamento_plano
            group by 2
            ) t1
        """.format(operadora=operadora, empresa_id=empresa_id)
        
        pessoas_mtm_prop = db.runQueryWithPandas(query)
        pessoas_mtm_prop = pessoas_mtm_prop['pessoas_mtm_prop'][0]

    except Exception as e:
        print(
            "Erro ao conectar no banco, talvez a coluna não exista nessa tabela")
        print(e)
        return df

    if pessoas_mtm_prop is None and not force_mtm:
        print("Lib: parse_mtm_from_db: mtm não aplicado, não há dados no banco para referência.")
        return df

    #Aplica MTM pela flag force_mtm
    if force_mtm:
        df = df_functions.transformar_mtm(df)
        print("Lib: parse_mtm_from_db: mtm aplicado, flag force_mtm")
        return df

    #Aplica MTM apenas se ultrapassar o threshold
    if pessoas_mtm_prop is not None:
        if pessoas_mtm_prop >= threshold:
            df = df_functions.transformar_mtm(df)
            print("Lib: parse_mtm_from_db: aplicado mtm, seguindo padrão do banco")
            return df
    print("Lib: parse_mtm_from_db: mtm não aplicado, seguindo padrão do banco")
    return df

def apply(df, *args, **kwargs):
    """
    Função alias para o método apply ser utilizado no pipe.

    Args:
        df: pandas.DataFrame
        column: str
            Coluna que será origem do valor, a qual o método apply será aplicado.
        destination: str, default `column`
            Coluna que será destino de valor do retorno do método apply.
        f: função, default id
           Função que será aplicada no apply da coluna.

    Returns:
        pandas.DataFrame
           Dataframe com apply aplicado.

    """
    column = kwargs.get("column", None)
    destination = kwargs.get("destination", column)
    f = kwargs.get("f", lambda x: x)

    assert column is not None, "coluna não informada."
    if column not in df:
        return df

    df[destination] = df[column].apply(f)

    return df


def applies(df, *args, **kwargs):
    """
    Função alias para o método apply ser utilizado no pipe
    em múltiplas colunas.

    Args:
        df: pandas.DataFrame
        columns: str
            Colunas que será origens do valor, as quais os métodos apply serão aplicados.
        destinations: str, default `columns`
            Colunas que serão destino do valor do retorno do método apply.
        f: função, default id
           Função que será aplicada no apply das colunas.

    Returns:
        pandas.DataFrame
           Dataframe com apply aplicado.

    """

    columns = kwargs.get("columns", [])
    destinations = kwargs.get("destinations", columns)
    f = kwargs.get("f", lambda x: x)

    assert columns is not None, "colunas não informadas."
    assert len(columns) == len(
        destinations
    ), "``columns` e `destinations`, precisam ter o mesmo tamanho"

    for column, destination in zip(columns, destinations):
        if column in df:
            df = df.assign(**{destination: lambda x: x[column].apply(f)})

    return df

def parse_date_legado(date):
    """
    Função para tratar datas (lib 1.0)

    Args:
        x: str|datetime, valor a ser tratado.

    Returns:
        datetime 
    """
    br_months = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez']

    if isinstance(date, str):
        date = date.strip()
    # Treats None
    if pd.isnull(date):
        return None
    # if type(date) == type(0.0) or type(date) == type(0) or (isinstance(date, str) and date.isnumeric()):
    #     import xlrd
    #     datetime_date = xlrd.xldate_as_datetime(int(date), 0)
    #     return datetime_date.date()

    # Treats 0
    try:
        if date == 0:
            print(date)
            return None

        # Treats datetime
        if type(date) == type(datetime(2010, 1, 1)):
            return date

        if type(date) != str:
            return date 

        # Treats 202012
        if len(re.findall(r"^20[1-2][0-9][0-1][0-9]$", date[:6])) != 0:
            return datetime.strptime(date[:6], '%Y%m') 

        # Treats 10/1/20, 12:00:00 AM 
        if len(re.findall(r"^[0-3]?[0-9]/[0-1]?[1-9]/[0-9]{2}\,\ [0-1]?[0-9]:(00:?){2} [A-Z]{2}$", date)) != 0:
            return datetime.strptime(date.split(',')[0], '%m/%d/%y')
        
        # Treats 08/03/21 00:00:00 
        if len(re.findall(r"^[0-3]?[0-9]/[0-1]?[1-9]/[0-9]{2}\ [0-1]?[0-9]:(00:?){2}", date)) != 0:
            return datetime.strptime(date[:8], '%m/%d/%y')
        
        # Treats 1999-01-31 00:00:00 
        if len(re.findall(r"^[1-2][0-9]{3}-[0-2][0-9]-[0-3][0-9]\ [0-9]{2}:[0-9]{2}:[0-9]{2}", date)) != 0:
            return datetime.strptime(date[:10], '%Y-%m-%d')

        # Treats 1999-01-31 
        if len(re.findall(r"^[1-2][0-9]{3}-[0-2][0-9]-[0-3][0-9]", date)) != 0:
            return datetime.strptime(date[:10], '%Y-%m-%d')

        # Treats 20/10/1000 13:42
        if len(str(date)) >= 10 and len(re.findall(r"^[0-9]{2}\/[0-1][0-9]\/[0-9]{4}", date[:10])) != 0:
            return datetime.strptime(date[:10], "%d/%m/%Y")
        
        # Treats 20/10/1000
        if len(str(date)) >= 10 and len(re.findall(r"^[0-9]{2}\/[0-1][0-9]\/[0-9]{4}$", date)) != 0:
            return datetime.strptime(date, "%d/%m/%Y")

        # Treats 12/1000
        if len(str(date)) >= 7 and len(re.findall(r"^[0-1][0-9]\/[0-9]{4}$", date)) != 0:
            return datetime.strptime("01/"+date, "%d/%m/%Y")

        # Treats Jan-20
        if len(re.findall(r"^[A-Z][a-z]{2}-[1-2][0-9]$", date)) != 0:
            month = br_months.index(date.split('-')[0].lower())+1
            date = str(month)+'-'+date.split('-')[1]
            return datetime.strptime(date, "%m-%y")

        # Treats 10/mar/1000
        if len(re.findall(r"^[0-9]{2}/[a-zA-Z]{3}/[0-9]{4}$", date)) != 0:
            return datetime.date(int(date[-4:]), br_months.index(date[:3])+1, 1)

        # Treats 26/07/2021 A 27/07/2021
        if ' A ' in date:
            if len(date) == len('26/07/2021 A 27/07/2021'):
                date_type = '%d/%m/%Y'
            elif len(date) == len('26/072021 A 27/07/2021'):
                date_type = '%d/%m%Y'
            else:
                raise ValueError('Não achei o date_type para data no formato "data_x A data_y"')
            return datetime.strptime(date.split(' A ')[0], date_type)
        
        # Treats 20/10/20
        if len(re.findall(r"^[0-9]{2}/0[1-9]|1[0-2]/[0-9]{2}$", date[:8])) != 0:
            if int(date[6:8]) > int(str(datetime.today().strftime("%Y"))[2:])+1:
                return datetime.strptime(date[:6]+'19'+date[6:8], "%d/%m/%Y")
            return datetime.strptime(date[:6]+'20'+date[6:8], "%d/%m/%Y")
        
        # Treats 12.2020.csv 12-2020.csv 12_2020.csv 12 2020.csv 122020.csv
        if len(re.findall(r"[0-1]{1}\d{1}[-_.\s]?[1-2]{1}[\d]{3}", date)) != 0:
            date_strip = re.findall(r"[0-1]{1}\d{1}[-_.\s]?[1-2]{1}[\d]{3}", date)
            date_strip = date_strip[0][-4:] + date_strip[0][:2] + '01'
            return datetime.strptime(date_strip, '%Y%m%d')
        
        # Treats Jan/20
        if len(re.findall(r"^[a-zA-Z]{3}\/[1-2][0-9]$", date)) != 0:
            month = br_months.index(date.split('/')[0].lower())+1
            date = str(month)+'/'+date.split('/')[1]
            return datetime.strptime(date, "%m/%y")
    except Exception as error:
        raise ValueError('Erro parse_date: ' + repr(error) + ' date: ' + date)
        
        
def transpose(
    left_df,
    right_df=None,
    db=None,
    tables=[],
    left_columns=[],
    right_columns=None,
    left_keys=[],
    right_keys=None,
    coalesce=True,
    invert=False,
    keep="first",
):
    """
    Função para transpor dados entre dataframes ou entre o banco e um dataframe 
    Args:
        left_df: pd.DataFrame, Dataframe origem.
        right_df: pd.DataFrame, Dataframe destino.
        db: WellbeDatabase, instância do banco.
        tables: List[str], tabelas que serão utilizadas na transposição caso seja uma transposição entre banco de dataframe.
        left_columns: List[str], Colunas que destino da transposição, serão adicionadas no left_df.
        right_columns: List[str], Colunas origem da transposição, são providas pelo right_df (default left_columns).
        left_keys: List[str], Chaves da transposição, valores da left_df. Análogo ao on do sql.
        right_keys: List[str], Chaves da transposição, valores da right_df (default left_keys). Análogo ao on do sql.
        coalesce: boolean, default True. Se deve ser preservado o valor de left_df caso o valor de right_df/database seja nulo.

    Returns:
        pandas.DataFrame
            Dataframe com a transposição feita.

    Examples:

        1) Transposição entre dois dataframes:
        ```python
            left_df.pipe(
                transpose,
                db=db,
                tables=["BI_Beneficiario", "BI_Colaborador"],
                left_columns=["nome_beneficiario", "nome_titular"],
                right_columns=["nome", "nome_titular"],
                left_keys=["cpf"],
                right_keys=["cod_associado"],
            )
        ```
        Transpõe o nome e nome do titular do banco usando o cpf do dataframe como chave e o cod_associado do banco como chave.
        
        2) Transposição entre o dataframe o banco:
        ```python
            left_df.pipe(
                transpose,
                right_df=beneficiarios,
                left_columns=["nome_beneficiario", "nome_titular"],
                right_columns=["nome", "nome_titular"],
                left_keys=["cpf"],
                right_keys=["cod_associado"],
            )
        ```
        Transpõe o nome e nome do titular do dataframe de beneficiários (right_df) usando o cpf do dataframe como chave e o cod_associado dos beneficiários como chave.

    """
    assert db is not None or right_df is not None, "Deve informar ao menos `db` ou `right_df`"

    if right_df is None and db is not None:
        select_query = ", ".join(right_keys + right_columns)
        group_query = ", ".join(right_keys)
        empresa_ids = ", ".join(left_df['empresa_id'].astype(str).unique())
        operadoras = ", ".join(list(map(lambda x: f"'{x}'", left_df['operadora'].unique())))

        query = "\nunion\n".join(
            [
                """
        select {select_query} 
            from {table} t1
            inner join (
                select max(id) id from {table}
                where empresa_id in ({empresa_ids})
                and operadora in ({operadoras})
                group by {group_query}
            ) t2 on t1.id = t2.id
            where empresa_id in ({empresa_ids}) 
              and operadora in ({operadoras})

        """.format(
                    select_query=select_query,
                    group_query=group_query,
                    operadoras=operadoras,
                    empresa_ids=empresa_ids,
                    table=table,
                )
                for table in tables
            ]
        )
        right_df = db.runQueryWithPandas(query)

    if right_columns is None:
        right_columns = left_columns

    if right_keys is None:
        right_keys = left_keys

    for left_key in left_keys:
        left_df[left_key] = left_df[left_key].astype(str)

    for right_key in right_keys:
        right_df[right_key] = right_df[right_key].astype(str)

    for left_column, right_column in zip(left_columns, right_columns):
        if left_column not in left_df:
            left_df[left_column] = None

        left_df = left_df.merge(
            right_df[right_keys + [right_column]].drop_duplicates(
                subset=right_keys, keep=keep
            ),
            left_on=left_keys,
            right_on=right_keys,
            how="left",
        )
        
        if left_column == right_column:
            left = "_x"
            right = "_y"

            if invert:
                left, right = right, left
        else:
            left, right = "", ""
        
        if coalesce:
            left_df[left_column] = left_df[right_column + right].combine_first(
                left_df[left_column + left]
            )
        else:
            left_df[left_column] = left_df[right_column + right]

        diff_keys = list(set(right_keys) - set(left_keys))

        if left_column == right_column:
            left_df = left_df.drop([left_column + left, right_column + right] + diff_keys, axis=1)
        else:
            left_df = left_df.drop([right_column + right] + diff_keys, axis=1)

    if not left_df.empty:
        left_df.replace('None', np.nan, inplace=True)

    return left_df

def drop_fcode_duplicated(file_paths=None, encoding='UTF-8'):
    """
    Função para remover arquivos duplicados.
    geralmente acontece nas extrações da amil.
    
    Args:
        file_paths: list, caminhos dos arquivos.
        encoding: str, encoding utilizado para abrir arquivo, por padrão UTF-8
    Returns:
        Um conjunto de caminhos de arquivos que estão aptos a serem subidos
    """
    if file_paths is None:
        print("lib: drop_fcode_duplicated: file_path vazio")
        return file_paths
    
    paths_lidos = []
    paths_validos = []
    
    for file_path in file_paths:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                file_contents = f.read()
                if file_contents not in paths_lidos:
                    paths_validos.append(file_path)
                paths_lidos.append(file_contents)
        except Exception as e:
            print(f"Erro arquivo: {file_path}")
            print(f"Exception: {e}")
            raise e

    print(f"lib: drop_fcode_duplicated: paths lidos: {len(paths_lidos)}")
    print(f"lib: drop_fcode_duplicated: paths válidos: {len(paths_validos)}")
    return paths_validos
