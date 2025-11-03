"""
O ideal para esse módulo seriam se todas as funções de processamento
aceitassem com parâmetro um dataframe (pd.DataFrame) e retornassem 
outro dataframe processado (pd.DataFrame). Para serem utilizados em
qualquer pipeline.

Existem exceções como os casos de leitura de arquivo.
"""

import unidecode
import pandas as pd
import numpy as np
import json
import hashlib
import os
import warnings
import re
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import text


import sys
from glob import glob
from tqdm.notebook import tqdm

warnings.filterwarnings("ignore")

def get_dados_empresas(db, empresas=[], empresa_id=True, corretora=None):
    """
    Função retorna lista de empresas com respectivos drive_folders.

    Args:
        db: WellbeDatabase
            Instância do banco.
        empresas: list
            Lista de strings com os nomes das empresas.
        empresa_id: bool
            Flag indicando se o retorno da funcao deve conter empresa_id.
    Returns:
        empresas:
            Lista de empresas contendo empresa id e drive_folder
        drive_folder:
            Lista de drive_folders das empresas
    """
    #     db = sqlalchemy.create_engine("mysql+pymysql://caina:K123da5F#DAS#@35.245.192.170:3306/stillgood")
    emps = [i.strip() for i in empresas.split('\n') if i.strip()]

    dados_empresa = []
    for emp in emps:
        if '(' in emp:
            index = emp.index('(')
            emp = emp[:index].strip()

        if emp[0] == '*':
            query = f"select * from BI_Empresa where nome like '{emp.replace('*', '')}'"
        else:
            query = f"select * from BI_Empresa where nome like '%{emp.replace('*', '')}%'"
        saida = None #db.getEngine().execute(query).fetchall()
        with db.getEngine().connect() as conn:
            saida = conn.execute(text(query)).fetchall()
        if len(saida) == 0:
            print(f'Empresa {emp}, nao encontrada')
        elif len(saida) > 1:
            if corretora != None:
                for i in range(len(saida)):
                    if corretora in str(saida[i][-2]):
                        dados_empresa.append(saida[i])
            else:
                print(f'Empresa {emp}, retornou mais de um resultado:', ', '.join(list(map(lambda x: str(x)[1], saida))))

        else:
            dados_empresa.append(saida[0])
    empresas = list(map(lambda x: [x[0], x[-5]], dados_empresa))

    if not empresa_id:
        return [i[1] for i in empresas]
    else:
        return empresas


def read_auto(
    path,
    encoding=None,
    sep=None,
    skiprows=None,
    skipfooter=None,
    **kwargs,
):
    """
    Função para abrir arquivos independente do formato ou encoding.

    Args:
        path: Path
        encoding: str
        sep: str
        skiprows: int (default auto|0)
        skipfooter: int (default auto|0)
        
    Returns:
        pandas.DataFrame
            Dataframe lido do arquivo.
    """
    def get_encoding(path):
        import chardet

        encodingdet = chardet.detect(open(path, "rb").read())
        assert (
            encodingdet["confidence"] > 0.33
        ), f"Não foi possível detectar automaticamente o encoding do arquivo {path}."
        encoding = encodingdet["encoding"]

        return encoding

    def get_sep(path, encoding="utf-8"):
        import csv

        dialect = csv.Sniffer().sniff((open(path, "r", encoding=encoding).read()))
        sep = dialect.delimiter

        if sep == " ":
            sep = "\t"

        return sep

    def get_skiprows(path, encoding=None, sep=None):
        import collections

        if encoding is None:
            encoding = get_encoding(path)

        if sep is None:
            sep = get_sep(path, encoding=encoding)

        plain = open(path, "r", encoding=encoding).read()
        sep_counts = [line.count(sep) for line in plain.split("\n")]
        sep_most_common, _ = collections.Counter(sep_counts).most_common(1)[0]

        # check
        max_total_seq = 1
        total_seq = 1
        for idx in range(len(sep_counts) - 1):
            if (
                sep_counts[idx] == sep_most_common
                and sep_counts[idx + 1] == sep_most_common
            ):
                total_seq += 1
            else:
                if total_seq > max_total_seq:
                    max_total_seq = total_seq
                total_seq = 1
        if total_seq > max_total_seq:
            max_total_seq = total_seq

        if max_total_seq / len(sep_counts) < 0.67:
            return 0

        skiprows = sep_counts.index(sep_most_common)

        return skiprows

    def get_skipfooter(path, encoding=None, sep=None):
        import collections

        if encoding is None:
            encoding = get_encoding(path)

        if sep is None:
            sep = get_sep(path, encoding=encoding)

        plain = open(path, "r", encoding=encoding).read()
        sep_counts = list(reversed([line.count(sep) for line in plain.split("\n")]))

        # remove \n
        if sep_counts[0] == 0:
            sep_counts = sep_counts[1:]

        sep_most_common, _ = collections.Counter(sep_counts).most_common(1)[0]

        # check
        max_total_seq = 1
        total_seq = 1
        for idx in range(len(sep_counts) - 1):
            if (
                sep_counts[idx] == sep_most_common
                and sep_counts[idx + 1] == sep_most_common
            ):
                total_seq += 1
            else:
                if total_seq > max_total_seq:
                    max_total_seq = total_seq
                total_seq = 1
        if total_seq > max_total_seq:
            max_total_seq = total_seq

        if max_total_seq / len(sep_counts) < 0.8:
            return 0

        skipfooter = sep_counts.index(sep_most_common)

        return skipfooter

    if "xls" in path:
        if skiprows is None:
            skiprows = 0

        df = pd.read_excel(path, skiprows=skiprows, **kwargs)
    else:
        if encoding is None:
            encoding = get_encoding(path)

        if sep is None:
            sep = get_sep(path, encoding=encoding)

        if skiprows is None:
            skiprows = get_skiprows(
                path, encoding=encoding, sep=sep            )

        if skipfooter is None:
            skipfooter = get_skipfooter(
                path, encoding=encoding, sep=sep            )

        df = pd.read_csv(path, encoding=encoding, sep=sep, skiprows=skiprows, **kwargs)
    if df.empty:
        raise pd.errors.EmptyDataError
    return df


def query(file_path, **kwargs):
    """
        ...
    """

    sql_folder = "sql"

    open_c = "[["
    close_c = "]]"

    open_v = "{{"
    close_v = "}}"


    f = open(os.path.join(sql_folder, file_path))
    assert f is not None, file_path + " inválido"

    raw = f.read()
    while open_c in raw:
        begin = raw.find(open_c)
        end = raw.find(close_c)

        sub = raw[begin : end + len(close_c)]
        while open_v in sub:
            begin_v = sub.find(open_v)
            end_v = sub.find(close_v)

            var = sub[begin_v : end_v + len(close_v)]
            var = var.replace(open_v, "").replace(close_v, "")
            if var in kwargs:
                sub = (
                    sub[0:begin_v]
                    + "'"
                    + str(kwargs.get(var))
                    + "'"
                    + sub[end_v + len(close_v) : len(sub)]
                )
            else:
                sub = ""

        sub = sub.replace(open_c, "").replace(close_c, "")

        raw = raw[0:begin] + sub + raw[end + len(close_c) : len(raw)]

    while open_v in raw:
        begin_v = raw.find(open_v)
        end_v = raw.find(close_v)

        var = raw[begin_v : end_v + len(close_v)]
        var = var.replace(open_v, "").replace(close_v, "")
        if var in kwargs:
            raw = (
                raw[0:begin_v]
                + "'"
                + str(kwargs.get(var))
                + "'"
                + raw[end_v + len(close_v) : len(raw)]
            )
        else:
            raise ValueError("Faltando parâmetro " + str(var))

    return raw


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

    df = df.rename(columns=lambda column: re.sub(' +', ' ', ' '.join(str(column).strip().split())))
    
    if invert:
        column_dict_inverted = {}
        for k, v in column_dict.items():
            for vv in v:
                column_dict_inverted[vv] = k
    else:
        column_dict_inverted = column_dict
        
    df = df.drop(df.columns.difference(list(column_dict_inverted)), 1)
    #column_dict_inverted = {k: v for k, v in column_dict_inverted.items() if k in df.columns}

    return df.rename(columns=column_dict_inverted)


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


def assign(df, *args, **kwargs):
    """
    Função para atribuir colunas novas com valores constantes.

    Args:
        df: pandas.DataFrame
        **kwargs: chave -> valor atribuído

    Returns:
        pandas.DataFrame
            Dataframe com as colunas atribuídas.


    Example:
        df.pipe(assign, empresa_id=4, operadora='Amil')
    """

    return df.assign(**kwargs)


def sexo_from_db(df, db=None, column=None, destination="sexo", threshold=0.8):
    """
    Função para inferir o sexo da pessoa através do valor da coluna passada.

    Args:
        df: pandas.DataFrame
        db: WellbeDatabase
            Instância do bd.
        column: str
            Coluna que será utilizada para inferir o valor.
        destination: str, default sexo
            Coluna destino.
        threshold: float, default 0.8
            Limiar de consenso que os valores do banco devem ter para a
            atribuição ser considerada verdadeira.

    Returns:
        pandas.DataFrame
            Dataframe com a coluna atribuída.

    """
    assert (
        db is not None
    ), "Necessário passar um objeto da classe WellbeDatabase como parâmetro"
    assert column is not None, "coluna não informada."

    names = set(df[column].apply(lambda y: y.split()[0].upper()))

    query = f"""
        select distinct fnome as nome, if(f * {threshold} <= m, 'Masculino', if(m * {threshold} <= f, 'Feminino', null)) as sexo
            from (
                 select fnome, m, f
                 from BI_Colaborador a
                          inner join (select substring_index(nome, ' ', 1) fnome,
                                             sum(sexo like 'M%')           m,
                                             sum(sexo like 'F%')           f
                                      from (select nome, sexo
                                            from BI_Colaborador
                                            union all
                                            select nome, sexo
                                            from BI_Beneficiario) x
                                      group by fnome
                                      order by fnome) b on substring_index(a.nome, ' ', 1) = fnome
             ) as fmf
        where fnome in ({','.join(map(lambda n: f"'{n}'",names))})
        group by fmf.fnome;
        """

    name_sexo_dict = {
        (e["nome"] if e["nome"] is not None else "").upper(): (
            e["sexo"].capitalize() if e["sexo"] is not None else None
        )
        for e in db.runQueryWithPandas(query).to_dict("records")
    }
    df = df.assign(
        **{
            destination: lambda x: x[column].apply(
                lambda y: name_sexo_dict.get(y.split()[0].upper())
            )
        }
    )
    df["sexo"] = df["sexo"].astype(str)

    name_sexo_none = len(df[df["sexo"] == "None"])
    if name_sexo_none > 0:
        print(
            f"Warning: Existem {name_sexo_none} registros sem atribuição de sexo. Necessário verificar."
        )

        # seria melhor refatorar isso em algum momento
        df[destination] = df[destination].astype(str)
        df[destination] = df[destination].apply(
            lambda s: s if str(s) not in [None, "nan", "None", "", "none"] else None
        )

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
        if "nome" in df :
            df["cpf"] = df["nome"].apply(md5)
        if "nome_beneficiario" in df :
            df["cpf"] = df["nome_beneficiario"].apply(md5)
        if "nome_titular" in df :
            df["cpf_titular"] = df["nome_titular"].apply(md5)
        return df

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
        if "nome" in df :
            df["cpf"] = df["nome"].apply(md5)
        if "nome_beneficiario" in df :
            df["cpf"] = df["nome_beneficiario"].apply(md5)
        if "nome_titular" in df :
            df["cpf_titular"] = df["nome_titular"].apply(md5)
        return df

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
        df["inclusao_plano"] = df["competencia"].apply(lambda x: parse_date(x))
        df["cancelamento_plano"] = df["competencia"].apply(lambda x: parse_date(x)
            + relativedelta(months=1)
            - relativedelta(days=1))
        print("Lib: parse_mtm_from_db: mtm aplicado, flag force_mtm")
        return df

    #Aplica MTM apenas se ultrapassar o threshold
    if pessoas_mtm_prop is not None:
        if pessoas_mtm_prop >= threshold:
            df["inclusao_plano"] = df["competencia"].apply(lambda x: parse_date(x))
            df["cancelamento_plano"] = df["competencia"].apply(lambda x: parse_date(x)
                + relativedelta(months=1)
                - relativedelta(days=1))
            print("Lib: parse_mtm_from_db: aplicado mtm, seguindo padrão do banco")
            return df
    print("Lib: parse_mtm_from_db: mtm não aplicado, seguindo padrão do banco")
    return df


# Apply
def parse_float(x):
    """
    Função para tratar float que usa , e . (lib 1.0)

    Args:
        x: int, valor a ser tratado.

    Returns:
        float 
    """
    if pd.isnull(x) or x == "-":
        return np.nan
        
    if type(x) == type(2.0):
        return float(x)

    if "." not in str(x) and "," not in str(x):
        return float(str(x))

    if "." in str(x) and "," in str(x):
        #Se ponto vem primeiro (99.999,99)
        if str(x).index(".") < str(x).index(","):
            return float(str(x).replace(".", "").replace(",", "."))
        #Se ponto vem primeiro (99,999.99)
        else:
            return float(str(x).replace(",", ""))

    if "," in str(x):
        if len(str(x)[(str(x).index(",")+1):]) < 3:
            return float(str(x).replace(",", "."))

    if "." in str(x):
        # if len(str(x)[(str(x).index(".")+1):]) < 3:
        return float(str(x))

    return float(x if type(x) == type(1) else str(x).replace(".", "").replace(",", "."))


def parse_float2(x):
    """
    Função para tratar pandas.Series float que usa , e .

    Args:
        x: pandas.Series, coluna a ser tratada.

    Returns:
        pandas.Series 
    """
    display(x)
    if str(x).contains(".", regex=False) and  str(x).contains(",", regex=False):
        print("TEM")
    return x


def parse_int(x, size=""):
    """
    Função para tratar int (lib 1.0)

    Args:
        x: int, valor a ser tratado.
        size: str, tamanho do int.

    Returns:
        str 
    """

    return x if pd.isnull(x) or not str(x).isnumeric() else ("{0:0=" + str(size) + "d}").format(int(x))


def parse_cpf(cpf):
    """
    Função para tratar cpfs (lib 1.0)

    Args:
        cpf: cpf, valor a ser tratado.

    Returns:
        str 
    """

    if pd.isnull(cpf):
        return cpf
    if type(cpf) == type(""):
        return int(cpf.replace(" ", "").replace("-", "").replace(".", ""))
    return int(cpf)


def parse_like_categorical(x, column_dict, ignore=False, show_logs=True):
    aux = [
        y
        for y in column_dict.keys()
        if y.lower() in unidecode.unidecode(str(x)).lower()
    ]
    if len(aux) > 0:
        return column_dict.get(aux[0], x)
    if ignore:
        if show_logs:
            print(f"Valor `{x}` não existe no dicionário")
        return x
    raise ValueError(f"Valor `{x}` não existe no dicionário")


def parse_sexo(x, ignore=False, show_logs=True):
    """
    Função para tratar sexos (lib 1.0)

    Args:
        x: str, valor a ser tratado.

    Returns:
        str 
    """
    if pd.isnull(x):
        print(f"Lib: parse_sexo: valor nulo")
        return x

    if str(x) in ('M', '1', '01', '1.0', '1.0000000000000000e+00'):
        x = 'Masculino'

    if str(x) in ('F', '0', '00', '0.0', '2', '2.0', '02', '2.0000000000000000e+00', '3'):
        x = 'Feminino'

    column_dict = {
        'mas': 'Masculino',
        'fem': 'Feminino'
    }

    return parse_like_categorical(x, column_dict=column_dict, ignore=ignore, show_logs=show_logs)


def parse_date(date):
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


def old_md5(x):
    """
    Função para tratar transformar str em md5 (lib 1.0)

    Args:
        x: str, valor a ser tratado.

    Returns:
        str (md5) 
    """
    if type(x) == str:
        return hashlib.md5(unidecode.unidecode(x).replace(" ", "").upper().encode("utf-8")).hexdigest()
    return None

def md5(x):
    """
    New_md5: transforma str em md5, substituindo conjunções que poderiam gerar pessoas duplicadas
    
    Args:
        x: x, valor a ser tratado.

    Returns:
        str (md5) 
    """
    if type(x) == str:
        x = x.replace(' e ', ' ').replace(' dos ', ' ').replace(' das ', ' ').replace(' do ', ' ').replace(' de ', ' ').replace(' da ', ' ')
        return hashlib.md5(unidecode.unidecode(x).replace(' ', '').upper().encode("utf-8")).hexdigest()
    return None

def check_field_diff(field, df, tables=None, db=None, show_logs=False, *args, **kwargs):
        "Deprecated"
        df.columns = [column.lower() for column in df.columns] # Empresa_ID
        distinct_group = df.groupby(
            ["empresa_id", "operadora"], as_index=False
        ).size()[["empresa_id", "operadora"]]

        distinct_emp_operadora = distinct_group.to_dict("records")
        distinct_emp_operadora_list = distinct_group.to_dict("list")

        empresa_ids = ",".join(
            map(lambda x: f"'{str(x)}'", distinct_emp_operadora_list["empresa_id"])
        )
        operadoras = ",".join(
            map(lambda x: f"'{x.lower()}'", distinct_emp_operadora_list["operadora"])
        )

        if tables is None:
            tables = ["BI_Eventos", "BI_Mensalidades", "BI_Colaborador", "BI_Beneficiario"]

        query = "\nunion\n".join(
            [
                """
        select empresa_id, operadora, {field}
            from {table}
            where operadora in ({operadoras})
              and Empresa_ID in ({empresa_ids})
            group by empresa_id, operadora, {field}
        """.format(
                    field=field,
                    operadoras=operadoras,
                    empresa_ids=empresa_ids,
                    table=table,
                )
                for table in tables
            ]
        )

        result = db.runQueryWithPandas(query)
        result["operadora"] = result["operadora"].apply(
            lambda x: x.decode("utf-8").lower() if isinstance(x, bytes) else str(x).lower()
        )

        for emp_op in distinct_emp_operadora:
            df_field_distinct_values = set(
                df[
                    (df["operadora"] == emp_op["operadora"])
                    & (df["empresa_id"] == emp_op["empresa_id"])
                ][field].unique()
            )

            db_field_distinct_values = set(
                result[
                    (result["operadora"] == emp_op["operadora"].lower())
                    & (result["empresa_id"] == emp_op["empresa_id"])
                    & ~(pd.isnull(result[field]))
                ][field]
            )

            if(show_logs):
                print(
                    f"Empresa: `{emp_op['empresa_id']}`, Operadora: `{emp_op['operadora']}`"
                )
                print(f"Campo `{field}`:")
                print(f"Database: ({', '.join(map(str, db_field_distinct_values))})")
                print(f"Dataframe: ({', '.join(map(str, df_field_distinct_values))})")
                print('\n')

            field_diff = df_field_distinct_values - db_field_distinct_values
            if len(field_diff) > 0:
                print(
                    f"Empresa: `{emp_op['empresa_id']}`, Operadora: `{emp_op['operadora']} possui diferença no campo `{field}`: ({', '.join(map(str, field_diff))})\n"
                )
                print(f"Database: ({', '.join(map(str, db_field_distinct_values))})")
                print(f"Dataframe: ({', '.join(map(str, df_field_distinct_values))})")
                print("\n")

        return df



def check_df_diff(df, db=None, tables=[], show_logs=False):
    "Deprecated"
    if db is not None and len(tables) > 0:
        fields = ['subestipulante', 'titularidade', 'internacao', 'sexo', 'categoria', 'plano', 'nome_plano']
        for field in fields:
            if field in df.columns:
                check_field_diff(field, df, db=db, tables=tables, show_logs=show_logs)


    return df


def summary(df, columns=[], key='empresa_id'):
    """
    Função que retorna um sumário do dataframe com base nas colunas
    informadas.

    Args:
        df: pandas.DataFrame
        columns: List[str]
        key: str, default: `empresa_id`. Chave que será utilizada com index no groupby

    Returns:
        pandas.DataFrame
            Dataframe agrupado de acordo com as colunas passadas.
    """

    try:
        return df.astype(str).melt(id_vars=key).drop_duplicates().pivot_table(
            index=key, columns="variable", values="value", aggfunc=",".join
        )[columns]
    except:
        columns = set(columns)
        df_columns = set(df.columns)
        diff = list(columns - df_columns)

        print("Não foi possível mostrar o sumário pois as colunas {} não existem".format(','.join(diff)))

        return pd.DataFrame()


def merge_many(df, column_key_value):
    "Deprecated"
    df = df.copy()
    df.fillna(0, inplace=True)
    df.reset_index(inplace=True, drop=True)
    if column_key_value not in df.columns:
        df[column_key_value] = 0
    colunas_valor = sorted(list(set([i if column_key_value in i else column_key_value for i in df.columns])), key=len)
    for colunas in colunas_valor[1:]:
        for linhas in range(len(df)):
            if df[column_key_value][linhas] == 0 and df[colunas][linhas] != 0:
                df.loc[linhas, column_key_value] = df[colunas][linhas]
        del df[colunas]
    return df

def merge_two(df, first_key, second_key):
    "Deprecated"
    df = df.copy()
    df.fillna(0, inplace=True)
    df.reset_index(inplace=True, drop=True)
    for i in range(len(df)):
        if df[first_key][i] == 0:
            df.loc[i, first_key] = df[second_key][i]
    del df[second_key]
    return df

def drop_zeros(df, column):
    "Deprecated"
    df = df.copy()
    df = df[df[column] != 0]
    df.reset_index(inplace=True, drop=True)
    return df

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

    return left_df


def set_empresa_id(df, empresa_id=None, column_dict={}):
    """
    DEPRECATED
    Função para setar o valor de empresa_id através do path do arquivo
    Args:
        df: pd.DataFrame.
        empresa_id: int, valor do empresa_id para forçar.
        column_dict: dict, dicionário com relação de path e id.

    Returns:
        pandas.DataFrame
            Dataframe com a coluna atribuída.

    """
    if empresa_id is not None:
        df["empresa_id"] = empresa_id
        return df

    return df.pipe(
        like_categorical,
        column="fcode",
        destination="empresa_id",
        column_dict=column_dict
    )

def set_empresa_id_from_db(df, db=None, empresa_id=None, debug=False):
    """
    Função para setar o valor de empresa_id através do path do arquivo
    Args:
        df: pd.DataFrame.
        empresa_id: int, valor do empresa_id para forçar.
        column_dict: dict, dicionário com relação de path e id.

    Returns:
        pandas.DataFrame
            Dataframe com a coluna atribuída.

    """
    if df.empty:
        print("Lib: set_empresa_id_from_db: datagrame vazio")
        return df  

    if empresa_id is not None:
        print(f"Lib: set_empresa_id_from_db: empresa_id = {empresa_id} passado como parâmetro")
        df["empresa_id"] = empresa_id
        return df
    
    if "fcode" not in df.columns:
        print("Lib: set_empresa_id_from_db: faltando fcode no dataframe")
        return df  

    def inner_parse_drive_folder(fcode):
        barras = [i for i, s in enumerate(fcode) if '/' in s]
        index_bases = fcode.index("/Bases")
        index_nome_empresa = barras[barras.index(index_bases)+3]
        return fcode[index_bases:index_nome_empresa].lower()

    fcodes = df.drop_duplicates(subset=["fcode"]).fcode.to_frame()
    fcodes["drive_folder"] = fcodes["fcode"].apply(lambda x: inner_parse_drive_folder(x))

    empresas = db.runQueryWithPandas(f"""
        select CONVERT(lower(drive_folder) USING utf8) as drive_folder,
            CAST(id AS CHAR)                           as id
        from BI_Empresa t1
        where CONVERT(lower(drive_folder) USING utf8) in {
               "('"+fcodes.drive_folder.unique()[0]+"')" if len(fcodes.drive_folder.unique())==1 else tuple(fcodes.drive_folder.unique())
                }
            group by 1,2
        """)
    fcodes = fcodes.merge(empresas, how='left', on='drive_folder')

    if debug:
        print("\n\nEMPRESAS NÃO ENCONTRADAS (drive_folder DIFERENTE DE BI_Empresa):")
        display(fcodes[fcodes["id"].isna()])

    fcodes = fcodes[fcodes["id"].notna()]
    
    if fcodes.empty:
        print("Lib: set_empresa_id_from_db: não foi possível identificar empresa_id pelo fcode, verifique a coluna drive_folder em BI_Empresa")
        return df

    #Faz um dict de fcode -> empresa_id
    dict_empresa_id = dict(zip(fcodes['fcode'], fcodes['id']))

    #Aplica o dict baseado na fcode preenchendo empresa_id nulo
    df['empresa_id'] = np.nan
    df['empresa_id'] = df['empresa_id'].fillna(df['fcode'].map(dict_empresa_id))
    
    print("Lib: set_empresa_id_from_db: empresa_id aplicado pelo fcode seguindo padrão do banco")

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
    
    carimbos = df[['empresa_id', 'operadora']].drop_duplicates(ignore_index=True)
    df['carimbo'] = np.nan
    for idx, row in carimbos.iterrows():
        carimbo = db.generateStamp(
            empresa_id=row['empresa_id'],
            arquivo=f"(Batch) {str(df.fcode.value_counts().idxmax())}",
            descricao=descricao,
            operadora=row['operadora'],
            script_version=script_version, 
            script_name=script_name,  
        )
        df.loc[(df['empresa_id']==row['empresa_id']) & (df['operadora']==row['operadora']), 'carimbo'] = carimbo

    return df

def categorical2(df, column=None, destination=None, column_dict={}):
    """
    Função para categorizar de acordo com o empresa_id.
    Utilizado em casos de mapeamento de subs que só pertence a uma empresa.

    Args:
        df: pd.DataFrame.
        column: str, nome da coluna a ser categorizada.
        destination: str, nome da coluna destino, default column.
        column_dict: dict, dicionário de mapeamentos.

    Returns:
        pandas.DataFrame
            Dataframe com a coluna categorizada.

    """
    if column is None:
        return df

    if column not in df.columns:
        print(f"Erro categorical2, coluna: {column} não esta no dataframe")
        return df

    if destination is None:
        destination = column
    
    if destination not in df.columns:
        df[destination]=None

    empresa_id = str(df["empresa_id"].value_counts().idxmax())
    column_dict = column_dict.get(empresa_id, {})

    df[destination] = df[destination].combine(df[column].apply(lambda x: column_dict.get(str(x), None)), lambda s1, s2: s1 if pd.isnull(s2) else s2)

    return df

def get_config(module):
    """
    Função para extrair arquivo de configuração da operadora.
    Args:
        module: str, nome do modulo.

    Returns:
        dict (json de configuração) 
    """
    temp = 'operadora2'
#     if not temp[1:] in module:
#         config_path = os.path.join(os.getcwd(), temp[1:], '/'.join(module.split('.')[:2]), 'config.json').replace(temp*2, temp)
#     else:
#         config_path = os.path.join('./', '/'.join(module.split('.')[:2]), 'config.json')
    parts = module.split('.')
    operadora2index = parts.index(temp)
    config_path = './' + '/'.join(parts[:operadora2index + 2]) + '/config.json'
    
    config_file = open(config_path, encoding='utf-8')
    return json.load(config_file)

def parse_status_unnimax(df):
    """
    Função para normalizar o campo status das empresas da corretora unnimax
    Args:
        df: pd.Dataframe

    Returns:
        o mesmo dataframe, normalizado ou não
    """
    if 'status' not in df.columns:
        # enquanto a operadora ainda usa o basepath em lib
        from lib.WellbeTest import error
        error('Coluna status não existe.')
        return df

    def inner(x):
        if pd.isnull(x):
            return "Excluído"

        if isinstance(x, str):
            y = x.lower()
            if "inativ" in y or "dem" in y or "apos" in y:
                return "Inativo"
            elif "ativ" in y:
                return "Ativo"
            else:
                return "Excluído"

        return "Excluído"

    df["status"] = df["status"].apply(inner)

    return df

def trim_all_columns(df):
    """
    DEPRECATED -> utilizar clear_df
    Função para remover espacos antes e depois de uma string/object
    aplicado em todas colunas do tipo string/object
    Args:
        df: pd.Dataframe

    Returns:
        o mesmo dataframe com as colunas tratadas
    """
    return df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

def clear_df(df):
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

def select_fcode_auto(db=None, table=None, column=None, operadora=None, fcodes=None, empresas=None, data_limite=None, debug=False):
    """
    Função para selecionar os arquivos a serem subidos 
    se baseando na última competência.
    
    **Necessário estar no caminho padrão do Drive e respeitar a 
      nomenclatura do banco (BI_Empresa.drive_folder)**.
    
    Args:
    	db: WellbeDatabase, instância do banco.
        table: str, tabela a qual será feita a busca pela última data inserida.
        column: str, coluna a qual será feita a busca pela última data inserida.
        operadora: str, operadora para busca no BD.
        fcodes: list, lista de diretórios dos arquivos a serem subidos.
        empresas: list, lista de empresa_id que serão retornadas.
        data_limite: str, data da maior competencia a ser retornada.
        debug: bool, ativa a opção de debug.
    Returns:
        Um conjunto de caminhos de arquivos que estão aptos a serem subidos
    """
    if fcodes is None or len(fcodes) == 0:
        print(f"lib: select_fcode_auto: parâmetro fcodes vazio")
        return []
        

    def inner_parse_drive_folder(fcode):
        barras = [i for i, s in enumerate(fcode) if '/' in s]
        index_bases = fcode.index("/Bases")
        index_nome_empresa = barras[barras.index(index_bases)+3]
        return fcode[index_bases:index_nome_empresa].lower()

    fcodes = pd.DataFrame(fcodes, columns=["fcode"])
    fcodes["drive_folder"] = fcodes["fcode"].apply(lambda x: inner_parse_drive_folder(x))

    to_ignore = []
    for file_path in fcodes["fcode"]:
        if re.search(r"(\/20[0-9][0-9]\/[0-1][0-9]\/)", file_path) is None:
            to_ignore.append(file_path)
    
    if debug:
        print("FILE PATHS IGNORADOS FORA DO PADRÃO DE DATA DOS DIRETÓRIOS ('*/ANO/MES/*'):")
        print(*to_ignore, sep = "\n")

    if len(to_ignore) > 0:
        fcodes = fcodes[~fcodes['fcode'].isin(to_ignore)]

    fcodes["file_data"] = fcodes["fcode"].apply(lambda x: re.search(r"(20[0-9][0-9]\/[0-1][0-9])", x)
        .group(0).replace('/', '-')+"-01")

    fcodes["file_data"] = fcodes["file_data"].apply(lambda x: datetime.strptime(str(x), "%Y-%m-%d"))

    max_datas = db.runQueryWithPandas(f"""
            select  CONVERT(lower(drive_folder) USING utf8) as drive_folder,
                    CAST(id AS CHAR) as id,
                    coalesce(max_data, STR_TO_DATE('1990-01-01', '%Y-%m-%d')) as max_data
            from BI_Empresa t1
         left join(select max({column}) as max_data,
                          empresa_id
                   from {table}
                   where operadora like '%{operadora}%'
                    and Empresa_ID in (select distinct id from BI_Empresa where operadora like '%{operadora}%')
                   group by 2
                   union
                   select max({column}) as max_data,
                          empresa_id
                   from {table}
                   where 1=1
                     and Empresa_ID = 12311238761287368172
                   group by 2) t2
                  on t1.id = t2.empresa_id
            where operadora like '%{operadora}%'
            and CONVERT(lower(drive_folder) USING utf8) in {
               "('"+fcodes.drive_folder.unique()[0]+"')" if len(fcodes.drive_folder.unique())==1 else tuple(fcodes.drive_folder.unique())
                }
            group by 1,2,3
        """)

    fcodes = fcodes.merge(max_datas, how='left', on='drive_folder')

    if debug:
        print("\n\nEMPRESAS NÃO ENCONTRADAS (drive_folder DIFERENTE DE BI_Empresa):")
        display(fcodes[fcodes["id"].isna()])

    fcodes = fcodes[fcodes["id"].notna()]
    fcodes["status"] = fcodes.apply(lambda x: 1 if x["file_data"]>x["max_data"] else 0, axis=1)

    if debug:
        print("\n\nRESULTADO DO PARSE DOS ARQUIVOS (drive_folder em lower case):")
        display(fcodes)

    if empresas is not None:
        fcodes=fcodes[fcodes["id"].isin(empresas)]
    
    if data_limite is not None:
        fcodes=fcodes[fcodes["file_data"] <= data_limite]

    return fcodes[fcodes["status"]==1]["fcode"].tolist()


def drop_fcode_duplicated(file_paths=None, encoding='UTF-8'):
    """
    Função para remover arquivos duplicados.
    geralmente acontece nas extrações da amil.
    
    Args:
        file_paths: list, caminhos dos arquivos.
        encoding (DEPRECATED): str, encoding utilizado para abrir arquivo, por padrão UTF-8
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
            with open(file_path, 'rb') as f:
                file_contents = f.read(256)
                # file_contents = f.read()
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


def calcular_sinistralidade_agregado(val, operadora=None, sinistralidade=None):
    """
    Função para calcular a sinistralidade de forma agregada por mês.
    Útil para quando o RG vem separado por subestipulante e se deseja calcular uma sinistralidade "total" por mês
    """
    if (sinistralidade == None) and (operadora == 'Amil'):
        def sinistralidade(sinistro, faturamento, coparticipacao):
            return sinistro / faturamento
    
    val.drop_duplicates(inplace=True, subset=['data', 'faturamento', 'sinistro', 'vidas', 'operadora', 'empresa_id'])
    val.sort_values(by=["data"], inplace=True, ignore_index=True)
    val['sinistralidade'] = 0
    data = val['data'][0]
    faturamento = 0
    sinistro = 0
    vidas = 0
    coparticipacao = 0
    
    for index in val.index:
        if val['data'][index] == data:
            faturamento += float(val['faturamento'][index])
            sinistro += float(val['sinistro'][index])
            vidas += int(val['vidas'][index])
            if 'coparticipacao' in val.columns:
                coparticipacao += float(val['coparticipacao'][index])
        else:
            val['sinistralidade'][index-1] = str(round(100*sinistralidade(sinistro, faturamento, coparticipacao), 2))
            val['faturamento'][index-1] = faturamento
            val['sinistro'][index-1] = sinistro
            val['vidas'][index-1] = vidas
            if 'coparticipacao' in val.columns:
                val['coparticipacao'][index-1] = coparticipacao
                coparticipacao = float(val['coparticipacao'][index])
            data = val['data'][index]
            faturamento = float(val['faturamento'][index])
            sinistro = float(val['sinistro'][index])
            vidas = int(val['vidas'][index])
    index += 1
    val['sinistralidade'][index-1] = str(round(100*sinistralidade(sinistro, faturamento, coparticipacao), 2))
    val['faturamento'][index-1] = faturamento
    val['sinistro'][index-1] = sinistro
    val['vidas'][index-1] = vidas
    if 'coparticipacao' in val.columns:
        val['coparticipacao'][index-1] = coparticipacao

    val = val[val['sinistralidade'] != 0]
    return val
