import pandas as pd
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import MonthEnd
import lib.legado.legado as legado
import re
import hashlib
import unidecode
import unicodedata
import lib.homonimos as ho
import glob

default_columns = {
    "parse_date": ["competencia", "data_realizacao", "data_exclusao", "inclusao_plano", "cancelamento_plano", "inclusao_plano_real", "cancelamento_plano_real", "data_de_nascimento", "nascimento", "data", "data_inicio", "data_fim", "data_pagamento", "inicio_internacao", "alta_internacao", "data_de_admissao", "data_inclusao"],
    "parse_float": ["valor", "valor_total", "coparticipacao", "valor_apresentado", "outros", "mensalidade", "total", "faturamento", "sinistro", "sinistralidade"],
    "parse_int": ["vidas", "idade", "quantidade", "empresa_id"],
    "remove_tracos_e_pontos": ["cpf", "cpf_titular", "matricula_familia", "cpf_real", "cpf_real_titular", "matricula", "cod_associado", "cod_plano", "codigo_procedimento", "guia"],
    "parse_nomes": ["nome", "nome_titular", "nome_beneficiario", "categoria_original", "subcategoria", "descricao", "plano", "nome_plano", "subestipulante", "logradouro", "bairro", "cidade", "estado_civil", "operadora_real", "executor", "especialidade", "tipo_sinistro", "detalhamento"],
    "zero_fillna": ["valor", "faturamento", "mensalidade", "coparticipacao", "outros"]
}

def decode_string(text):
    if text is None:
        return ''
    try:
        text = str(text, 'utf-8')
    except (TypeError, UnicodeDecodeError):
        pass
    text = unicodedata.normalize('NFD', str(text))
    text = text.encode('ascii', 'ignore').decode("utf-8")
    return str(text)


def general_assign(df, **kwargs):
    """
    Define o fcode, empresa_id e operadora do dataframe.
    Se não informado empresa_id, procura o id em BI_Empresa, usando o file_path.
    Se não informado operadora, procura a operadora no file_path
    """
    file_path = kwargs.get('file_path')
    empresa_id = kwargs.get('empresa_id', None)
    operadora = kwargs.get('operadora', None)
    db = kwargs.get('db', None)
    debug = kwargs.get('debug', False)
    if df.empty:
        print("general_assign: dataframe vazio")
        return df, None
    if 'fcode' not in df.columns:
        df['fcode'] = file_path
    barras = [i for i, s in enumerate(file_path) if '/' in s]
    if empresa_id is None:
        assert "/Bases" in file_path, "general_assign: '/Bases' não foi encontrado ao analisar o file_path"
        index_bases = file_path.index("/Bases")
        assert len(barras) >= barras.index(index_bases) + 2, "general_assign: não foi possível separar o file_path até o nome da empresa"
        index_nome_empresa = barras[barras.index(index_bases) + 3]
        drive_folder = file_path[index_bases:index_nome_empresa].lower()
        drive_folder = decode_string(drive_folder).lower()
        empresas = db.runQueryWithPandas(f"""
            select CAST(id AS CHAR) as id, drive_folder
            from BI_Empresa where drive_folder is not null
            """)
        empresas['drive_folder'] = empresas['drive_folder'].apply(lambda x: x if pd.isnull(x) else decode_string(x).lower())
        empresa = empresas.loc[empresas.drive_folder == drive_folder]
        assert not empresa.empty, "general_assign: não foi possível identificar empresa_id pelo file_path, verifique a coluna drive_folder em BI_Empresa"
        assert empresa.shape[0] == 1, "general_assign: mais de um id identificado para o file_path informado, verifique em BI_Empresa"
        empresa_id = empresa["id"].iloc[0]
        print(f"general_assign: empresa_id = {empresa_id} aplicado pelo fcode seguindo padrão do banco")
    else:
        print(f"general_assign: empresa_id = {empresa_id} passado como parâmetro")
    if operadora is None:
        assert "/Bases" in file_path, "general_assign: '/Bases' não foi encontrado ao analisar o file_path"
        index_bases = file_path.index("/Bases")
        assert len(barras) >= barras.index(index_bases) + 3, "general_assign: não foi possível separar o file_path até o nome da operadora"
        index_operadora_inicio = barras[barras.index(index_bases) + 3] + 1
        index_operadora_fim = barras[barras.index(index_bases) + 4]
        operadora = file_path[index_operadora_inicio : index_operadora_fim]
        print(f"general_assign: operadora = '{operadora}' identificado pelo fcode")
    df['operadora'] = operadora
    df['empresa_id'] = empresa_id
    return df, empresa_id

def new_md5(df, **kwargs):
    def inner(x):
        if pd.isnull(x):
            return x
        x = str(x).replace(' e ', ' ').replace(' dos ', ' ').replace(' das ', ' ').replace(' do ', ' ').replace(' de ', ' ').replace(' da ', ' ').replace(' ','')
        return hashlib.md5(unidecode.unidecode(x).upper().encode("utf-8")).hexdigest()


    if 'cpf' in df.columns and 'cpf_real' not in df.columns:
        if not len(re.findall(r"([0-9a-f]{32})", str(df['cpf'].iloc[0]))) == 1:
            df['cpf_real'] = df['cpf']
    if 'cpf_titular' in df.columns and 'cpf_real_titular' not in df.columns:
        if not len(re.findall(r"([0-9a-f]{32})", str(df['cpf_titular'].iloc[0]))) == 1:
            df['cpf_real_titular'] = df['cpf_titular']
    if 'nome' not in df.columns and 'nome_beneficiario' not in df.columns:
        print(f'apply_md5: Coluna "nome/nome_beneficiario" não está no df')
    for column in ['nome', 'nome_beneficiario']:
        if column in df.columns:
            df['cpf'] = df[column].apply(inner)
    if 'nome_titular' not in df.columns:
        print(f'apply_md5: Coluna "nome_titular" não está no df')
    else:
        df['cpf_titular'] = df['nome_titular'].apply(inner)
    return df

def split_colab_benes(df, **kwargs):
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()
    elif 'titularidade' not in df.columns:
        raise Exception(f'split_colab_benes: Coluna {column} não está no df')
    cols = df[df["titularidade"] == "titular"].copy(deep=True)
    benes = df[df["titularidade"] != "titular"].copy(deep=True)
    if 'data_de_nascimento' in benes.columns:
        benes.rename(columns={"data_de_nascimento": "nascimento"}, inplace=True)
    if 'nascimento' in cols.columns:
        cols.rename(columns={"nascimento": "data_de_nascimento"}, inplace=True)
    if 'cargo' in benes.columns:
        del benes['cargo']
    return cols, benes

def parse_date(df, **kwargs):
    columns = kwargs.get('columns', default_columns["parse_date"])
    formato = kwargs.get('formato', None)
    debug = kwargs.get('debug', False)
    def inner(x, formato=None):
        if pd.isnull(x):
            return pd.NaT
        if isinstance(x, datetime.datetime):
            return x
        try:
            x_backup = x
            x = str(x).strip()
            if not formato is None:
                return pd.to_datetime(x, format=formato)
            else:
                return legado.parse_date_legado(x)
        except:
            print('parse_date: Não foi possível parsear a data', str(x_backup))
            return x
    for column in columns:
        if column in df.columns:
            df[column] = df[column].apply(inner, formato=formato)
            if column in ['data_de_nascimento', 'nascimento']:
                df[column] = df[column].apply(lambda x: x if pd.isnull(x) else x + relativedelta(years=-100) if x > datetime.datetime.today() else x)
        elif debug:
            print(f'parse_date: Coluna {column} não está no df')

    return df

def parse_sexo(df, **kwargs):
    dicts = kwargs.get('dicts', {})
    def inner(x):
        for k, v in dicts.items():
            if x == k:
                return v
        if pd.isnull(x):
            return x
        x_backup = x
        x = str(x).strip().lower()
        if x.startswith('m'):
            return 'Masculino'
        if x.startswith('f'):
            return 'Feminino'
        #print('parse_sexo: Valor de sexo desconhecido: ', str(x_backup))
        return x_backup
    if 'sexo' in df.columns:
        df['sexo'] = df['sexo'].apply(inner)
    else:
        print('parse_sexo: Coluna sexo não está no df')
    return df

def parse_float(df, **kwargs):
    columns = kwargs.get('columns', default_columns["parse_float"])
    columns_fillna = kwargs.get('columns', default_columns["zero_fillna"])
    round_values = kwargs.get('round_values', False)
    multiply_sinistralidade = kwargs.get('multiply_sinistralidade', False)
    debug = kwargs.get('debug', False)
    def inner(x, column=None):
        if pd.isnull(x) or isinstance(x, float):
            return x
        x_backup = x
        x = str(x).replace('%','').replace('R$','').strip()
        if "," in x and "." in x:
            # Se a vírgula vem primeiro (1,000.00)
            if str(x).index(",") < str(x).index("."):
                return float(x.replace(",", ""))
            else:
                return float(x.replace(".", "").replace(",", "."))
        # Se só tem vírgula (100,00)
        if "," in x:
            return float(x.replace(",", "."))
        # Se só tem ponto (100.00) ou nada (100)
        try:
            return float(x)
        except:
            print('parse_float: Não foi possível converter para float:', str(x_backup))
            return x_backup
    for column in columns:
        if column in df.columns:
            df[column] = df[column].apply(inner, column=column)
            if multiply_sinistralidade and column == "sinistralidade":
                df["sinistralidade"] = 100 * df["sinistralidade"]
            if round_values:
                df[column] = df[column].apply(lambda x: x if pd.isnull(x) else round(x, round_values))
        elif debug:
            print(f'parse_float: Coluna {column} não está no df')
    for column in columns_fillna:
        if column in df.columns:
            df[column] = df[column].fillna(0)
    return df


def parse_nome_upper(df, **kwargs):
    columns = kwargs.get('columns', default_columns["parse_nomes"])
    debug = kwargs.get('debug', False)
    for column in columns:
        if column in df.columns:
            df[column] = df[column].apply(lambda x: unidecode.unidecode(str(x)) if (pd.isna(x) == False) else x)
            df[column] = df[column].apply(lambda x: re.sub('\\s+', ' ', x.upper().strip()) if (pd.isna(x) == False) else x)
            df[column] = df[column].apply(lambda x: str(x).replace('\'',''))
            df[column] = df[column].apply(lambda x: str(x).strip(' '))
    return df

def parse_int(df, **kwargs):
    columns = kwargs.get('columns', default_columns["parse_int"])
    debug = kwargs.get('debug', False)
    stringify = kwargs.get('stringify', False)
    def inner(x):
        if pd.isnull(x) or isinstance(x, int):
            return x
        x_backup = x
        x = str(x).strip()
        x = re.sub(r"[\.|\,]0+$", '', x).replace('.','')
        try:
            if stringify:
                return x
            else:
                return int(x)
        except:
            print('parse_int: Não foi possível converter para int:', str(x_backup))
            return x_backup
    for column in columns:
        if column in df.columns:
            df[column] = df[column].apply(inner)
        elif debug:
            print(f'parse_int: Coluna {column} não está no df')
    return df

def parse_competencia(df, **kwargs):
    file_path = kwargs.get('file_path', None)
    tabela = kwargs.get('tabela', None)
    debug = kwargs.get('debug', False)
    force = kwargs.get('force', False)
    def inner(x):
        try:
            return pd.to_datetime(re.search(r"(20[0-9][0-9]\/[0-1][0-9])",
                                            x.replace('\\','/')).group(0).replace('/', '-')+"-01")
        except:
            print("parse_competencia: não foi possível determinar a competencia através do file_path do arquivo, verifique se ano/mês estão no caminho.")
            return None
    if (tabela == 'BI_Mensalidades') or ("mensalidade" in df.columns):
        column = "data_realizacao"
    elif tabela in ('BI_Validador', 'BI_Faturamento'):
        column = "data"
    else:
        column = "competencia"
    if column not in df.columns or force:
        if file_path is None:
            if 'fcode' in df.columns:
                if len(df['fcode'].unique()) == 1:
                    df[column] = inner(df['fcode'].unique()[0])
                else:
                    df[column] = df['fcode'].apply(inner)
            elif debug:
                print('parse_competencia: file_path não informado e fcode não está no df')
        else:
            df[column] = inner(file_path)
    elif debug:
        print(f'parse_competencia: Coluna {column} já está no df')
    return df

def parse_titularidade(df, **kwargs):
    dicts = kwargs.get('dicts', {})
    null_eh_titular = kwargs.get('null_eh_titular', False)
    outras_titularidades = kwargs.get('outras_titularidades', True)
    def inner(x):
        for k, v in dicts.items():
            if x == k:
                return v
        if pd.isnull(x):
            if null_eh_titular:
                return 'Titular'
            return x
        x_backup = x
        x = str(x).lower().strip().replace(' ','')
        if x in ['t', 'tit', 'titular', 'titularbeneficiário(a)', 'titularbeneficiario(a)', 'titula', '0-Titular']:
            return 'titular'
        elif (not outras_titularidades) or (x == 'dependente') or (x == 'depend'):
            return 'dependente'
        elif x in ['filh','3-Filho(a)','filho','filha', 'filho adotivo','filho adotiva', 'filho.filha', 'filho(a) invalido(a)', 
                   'filhas', 'filhos','filhas adotivas', 'filhos adotivos', "filho(a) adotivo(a)"]:
            return 'filho(a)'
        elif x in ['pai', 'mae', 'mãe', '5-Mãe']:
            return 'pai.mãe'
        elif x in ['companh','njuge','espos','esposo','marid','conjug','2-Companheiro(a)','1-Cônjuge', 'cônjuge','esposa','marido','companheira', 'Conjuge', 'companheiro(a)', 'conjuge']:
            return 'companheiro(a)'
        elif x in ['agreg','agregado']:
            return 'agregado(a)'
        elif 'curatela' in x:
            return 'curatelado(a)'
        elif 'entead' in x:
            return 'enteado(a)'
        elif 'sogr' in x:
            return 'sogro(a)'
        elif 'irm' in x:
            return 'irmão(ã)'
        elif 'sobrinh' in x:
            return 'sobrinho(a)'
        elif 'cunhad' in x:
            return 'cunhado(a)'
        elif x in ['neto', 'neta', 'netos', '6-neto(a)']:
            return 'neto(a)'
        elif x in ['tutelad','tutela']:
            return 'tutelado(a)'
        elif x in ['outr','indefi','mengua','9-Outros', "outra dependencia", "outros", "invalido?",  "outra dependência"]:
            return 'outro'
        else:
            print('parse_titularidade: Valor desconhecido:', str(x_backup))
            return x_backup
    if 'titularidade' in df.columns:
        # backup de multiplas titularidades em parentesco
        if ('parentesco' not in df.columns) and len(df[df['titularidade'].notna()]['titularidade'].unique()) > 2:
            df['parentesco'] = df['titularidade']
        df['titularidade'] = df['titularidade'].apply(inner)
    else:
        if 'parentesco' in df.columns:
            df['titularidade'] = df['parentesco'].apply(inner)
        else:
            print('parse_titularidade: Coluna titularidade/parentesco não está no df')
    return df

def remove_tracos_e_pontos(df, **kwargs):
    columns = kwargs.get('columns', default_columns["remove_tracos_e_pontos"])
    debug = kwargs.get('debug', False)
    def inner(x):
        if pd.isnull(x):
            return x
        # x = str(x).replace(' ','').replace('-','')
        # x = re.sub(r"[\.|\,]0+$", '', x).replace('.','')
        x = re.sub(r"[\.|\,|\-|\s|\"|\=|\']", '', str(x))
        return x
    for column in columns:
        if column in df.columns:
            df[column] = df[column].apply(inner)
        elif debug:
            print(f'remove_tracos_e_pontos: Coluna {column} não está no df')
    return df

def titular_encadeado(df, **kwargs):
    overwrite = kwargs.get('overwrite', False)
    for column in ['nome_titular', 'cpf_titular']:
        if (column in df.columns):
            if not overwrite:
                print(f'titular_encadeado: Coluna {column} já está no df, retornando df')
                return df
            else:
                print(f'titular_encadeado: Coluna {column} já está no df, overwriting')
    for column in ['titularidade', 'nome', 'cpf']:
        if (column not in df.columns):
            raise Exception(f'titular_encadeado: Coluna {column} não está no df, retornando df')
    pd.options.mode.chained_assignment = None # disabling false positive warning
    df['nome_titular'] = None
    df['cpf_titular'] = None
    nome_titular = None
    cpf_titular = None
    for index in df.index:
        if df['titularidade'][index] == 'Titular':
            nome_titular = df['nome'][index]
            cpf_titular = df['cpf'][index]
        df['nome_titular'][index] = nome_titular
        df['cpf_titular'][index] = cpf_titular
    pd.options.mode.chained_assignment = 'warn' # enabling the warning again
    return df

def transformar_mtm(df, **kwargs):
    considerar_status = kwargs.get("considerar_status", False)

    if 'competencia' not in df.columns:
        print('transformar_mtm: Coluna competencia não está no df')
        return df

    # assert len(df["competencia"].unique()) == 1
    # competencia = pd.to_datetime(df["competencia"].unique()[0])

    # salva os valores nas colunas reais caso estas ainda não existam e seja detectado que os valores atuais são reais
    if ('inclusao_plano' in df.columns) and ('inclusao_plano_real' not in df.columns):
        if len(df['inclusao_plano'].unique()) > 5:
            df['inclusao_plano_real'] = df['inclusao_plano']

        if ('cancelamento_plano' in df.columns) and ('cancelamento_plano_real' not in df.columns):
            df['cancelamento_plano_real'] = df['cancelamento_plano']

    df["inclusao_plano"] = df["competencia"]
    if considerar_status and ("status" in df.columns):
        df["cancelamento_plano"] = df["competencia"]
        df.loc[df["status"] == 'Ativo', ["cancelamento_plano"]] = df.loc[df["status"] == 'Ativo', ["competencia"]] + MonthEnd(1)
    else:
        df["cancelamento_plano"] = df["competencia"] + MonthEnd(1)
    return df


def parse_mtm_from_db_com_status(df, **kwargs):
    db = kwargs.get("db")
    force_mtm = kwargs.get("force_mtm", False)
    threshold = kwargs.get("threshold", 0.25)
    sobrescrever_status = kwargs.get("sobrescrever_status", False)

    """
    1. se status não estiver no df, infere status a partir da coluna cancelamento_plano_real (comparando com a competencia)
    2. verifica se a empresa está MTM no banco ou flag force_mtm == True
    3. ao aplicar MTM, cancelamento_plano = inclusao_plano onde status != 'Ativo'
        cancelamento_plano = last_day(inclusao_plano) onde status == 'Ativo'
    """
    if "operadora" not in df:
        print("parse_mtm_from_db_com_status: coluna 'operadora' não está no df")
        return df
    if "empresa_id" not in df:
        print("parse_mtm_from_db_com_status: coluna 'empresa_id' não está no df")
        return df
    if "competencia" not in df:
        print("parse_mtm_from_db_com_status: coluna 'competencia' não está no df")
        return df

    # Inferência do status
    if "status" in df.columns and not sobrescrever_status:
        print("parse_mtm_from_db_com_status: coluna 'status' já está no df e sobrescrever = False, pulando inferência de status")
    else:
        if "cancelamento_plano_real" not in df.columns:
            print("parse_mtm_from_db_com_status: coluna 'cancelamento_plano_real' não está no df, não é possível inferir o status")
        else:
            def inner(x):
                if pd.isnull(x['cancelamento_plano_real']):
                    return "Ativo"
                if x['cancelamento_plano_real'] <= x['competencia']:
                    return "Não ativo"
                return "Ativo"
            print("parse_mtm_from_db_com_status: status inferido através da coluna cancelamento_plano_real")
            df["status"] = df.apply(inner, axis=1)

    # parse_mtm_from_db
    operadora = df["operadora"].value_counts().idxmax()
    empresa_id = df["empresa_id"].value_counts().idxmax()
    try:
        query = f"""
        select sum(if(t1.data_dif = 1, pessoas, 0)) / sum(pessoas) as pessoas_mtm_prop
        from (select count(distinct cpf)                                         pessoas,
                     TIMESTAMPDIFF(MONTH, inclusao_plano, DATE_ADD(cancelamento_plano, INTERVAL 3 DAY)) as data_dif
            from BI_Colaborador
            where operadora like '%%{operadora}%%'
                and Empresa_ID = {empresa_id}
                and inclusao_plano is not null
                and cancelamento_plano is not null
                and inclusao_plano <> cancelamento_plano
            group by 2
            union all
            select count(distinct cpf)                                         pessoas,
                   TIMESTAMPDIFF(MONTH, inclusao_plano, DATE_ADD(cancelamento_plano, INTERVAL 3 DAY)) as data_dif
            from BI_Beneficiario
            where operadora like '%%{operadora}%%'
                and Empresa_ID = {empresa_id}
                and inclusao_plano is not null
                and cancelamento_plano is not null
                and inclusao_plano <> cancelamento_plano
            group by 2
            ) t1
        """

        pessoas_mtm_prop = db.runQueryWithPandas(query)
        pessoas_mtm_prop = pessoas_mtm_prop['pessoas_mtm_prop'][0]

    except Exception as e:
        print(
            "parse_mtm_from_db_com_status: Erro ao conectar no banco, talvez a coluna não exista nessa tabela")
        print(e)
        return df

    if pessoas_mtm_prop is None and not force_mtm:
        print("parse_mtm_from_db_com_status: mtm não aplicado, não há dados no banco para referência.")
        return df

    #Aplica MTM pela flag force_mtm
    if force_mtm:
        df = transformar_mtm(df, considerar_status=True)
        print("parse_mtm_from_db_com_status: mtm aplicado, flag force_mtm")
        return df

    #Aplica MTM apenas se ultrapassar o threshold
    if pessoas_mtm_prop is not None:
        if pessoas_mtm_prop >= threshold:
            df = transformar_mtm(df, considerar_status=True)
            print("parse_mtm_from_db_com_status: aplicado mtm, seguindo padrão do banco")
            return df
    print("parse_mtm_from_db_com_status: mtm não aplicado, seguindo padrão do banco")

    # falta abrir o último mes automaticamente...
    return df


def save_columns(df, **kwargs):
    # se cpf ainda não está com md5, salva a coluna cpf em cpf_real (se ainda não estiver preenchida)
    if 'cpf' in df.columns and 'cpf_real' not in df.columns:
        if not len(re.findall(r"([0-9a-f]{32})", str(df['cpf'].iloc[0]))) == 1:
            df['cpf_real'] = df['cpf']

    # se cpf_titular ainda não está com md5, salva a coluna cpf_titular em cpf_real_titular (se ainda não estiver preenchida)
    if 'cpf_titular' in df.columns and 'cpf_real_titular' not in df.columns:
        if not len(re.findall(r"([0-9a-f]{32})", str(df['cpf_titular'].iloc[0]))) == 1:
            df['cpf_real_titular'] = df['cpf_titular']

    # salva os valores nas colunas reais caso estas ainda não existam e seja detectado que os valores atuais são reais
    if ('inclusao_plano' in df.columns) and ('inclusao_plano_real' not in df.columns):
        if len(df['inclusao_plano'].unique()) > 5:
            df['inclusao_plano_real'] = df['inclusao_plano']

        if ('cancelamento_plano' in df.columns) and ('cancelamento_plano_real' not in df.columns):
            df['cancelamento_plano_real'] = df['cancelamento_plano']

    if 'categoria_original' not in df.columns:
        if 'subcategoria' in df.columns:
            df['categoria_original'] = df['subcategoria']
        elif 'categoria' in df.columns:
            df['categoria_original'] = df['categoria']
    return df

def drop_columns(df, **kwargs):
    columns = kwargs.get('columns', False)
    if not columns:
        print(f'drop_columns: Lista "columns" não foi informada, retornando df')
    else:
        df = df.drop(columns=columns)
    return df

def drop_duplicated(df, **kwargs):
    subset = kwargs.get('subset', None)
    keep = kwargs.get('keep', 'first')
    return df.drop_duplicates(subset=subset, keep=keep, inplace=False, ignore_index=False)

def inferir_status(df, **kwargs):
    sobrescrever_status = kwargs.get("sobrescrever_status", False)
    if "status" in df.columns and not sobrescrever_status:
        print("inferir_status: coluna 'status' já está no df e sobrescrever_status = False, retornando df")
        return df
    if "cancelamento_plano_real" not in df.columns:
        print("inferir_status: coluna 'cancelamento_plano_real' não está no df, não foi possível inferir o status")
        return df
    if "competencia" not in df:
        print("inferir_status: coluna 'competencia' não está no df, não foi possível inferir o status")
        return df
    def inner(x):
        if pd.isnull(x['cancelamento_plano_real']):
            return "Ativo"
        if x['cancelamento_plano_real'] <= x['competencia']:
            return "Não ativo"
        return "Ativo"
    print("inferir_status: inferindo o status através da coluna cancelamento_plano_real")
    df["status"] = df.apply(inner, axis=1)
    return df

def fix_mensalidades(db=None, df=None, force=False):
    """
    Função retorna diferença entre mensalidade e validador.
    Funciona para quando o DF contém várias empresas da mesma operadora (Padrão Op1, Op2, Op3).
    Não implementado para múltiplas empresas e múltiplas operadoras (Necessário adaptar o for loop do script adicionando mais um for aninhado).
    Args:
        db: WellbeDatabase
            Instância do banco.
        df: DataFrame
            DataFrame de mensalidades.
        force: bool
            Flag que indica se a correçao vai ser para todos os meses ou apenas os menores que 5%.
    Returns:
        df:
            DataFrame com diferenca de mensalidades
    """
    print('Executando correções de mensalidade')
    #Check das colunas
    columns_check = ['empresa_id', 'operadora', 'data_realizacao', 'mensalidade', 'coparticipacao', 'outros']
    columns_availabe = []
    for col in columns_check:
        if col in df.columns:
            columns_availabe.append(col)

    unavailable = list(set(columns_check) - set(columns_availabe))
    if len(unavailable) > 1:
        print(f"Colunas indisponíveis: {','.join(list(set(columns_check) - set(columns_availabe)))}")
    if 'mensalidade' not in df.columns:
        print('Coluna mensalidade não está no DataFrame')
        return

    for empresa_id in df.empresa_id.unique():
        #Descobrindo a dash para o cáculo de mensalidade
        operadora = df.operadora.unique()[0]
        query = f"""
            select sql_no_cache {empresa_id} as empresa_id,
                    '{operadora}' as operadora,
                    COALESCE(dados_empresas.painel_mensalidades, empresa.dashboard_mensalidades) as dash
            FROM (select {empresa_id} as empresa_id, '{operadora}' as operadora) eve
            left join(select Empresa_ID, operadora, painel_mensalidades 
                        from BI_Dados_Empresas 
                        where Empresa_ID = {empresa_id}
                        and operadora = '{operadora}'
                        group by 1,2,3) dados_empresas
                        on eve.empresa_id = dados_empresas.empresa_id and eve.operadora = dados_empresas.operadora
            left join(select id, dashboard_mensalidades 
                        from BI_Empresa 
                        where id = {empresa_id}
                        group by 1,2) empresa
                    on eve.empresa_id = empresa.id
        """
        dash = db.runQueryWithPandas(query)

        if  len(dash['dash'].unique()) > 1:
            print(f'Mais de uma dash identificada para empresa:{empresa_id}, operadora:{operadora}')
            return
        dash = dash['dash'].unique()[0]
        print(f'Dash: {dash}', end=' -> ')

        #Caso calculo da mensalidade siga outra lógica adicionar mais um if, se não, adicionar dash a lista
        if dash in [2,4,7,9,10,11,12,13]:
            print('Calculo de mensalidade: mens_fat+mens_outros')
            if 'outros' in df.columns:
                df['mens_total'] = df[['mensalidade', 'outros']].sum(axis=1)
            else:
                df['mens_total'] = df['mensalidade']
        else:
            print('Dash mensalidade não identificada -> Calculo de mensalidade (default): mens_fat+mens_outros')
            if 'outros' in df.columns:
                df['mens_total'] = df[['mensalidade', 'outros']].sum(axis=1)
            else:
                df['mens_total'] = df['mensalidade']

        #Total do DataFrame
        total_df = df[['data_realizacao', 'empresa_id', 'operadora', 'mens_total']].groupby(['data_realizacao', 'empresa_id', 'operadora']).sum().reset_index()
        display(total_df)

        del df['mens_total']

        #Buscando dados validador
        query = f"""
            select sql_no_cache data as data_realizacao, sum(faturamento) as faturamento
            from BI_Validador 
            where Empresa_ID = {empresa_id}
                  and operadora = '{operadora}'
                  and data in ({
        ','.join(["'"+str(data)+"'" for data in total_df.data_realizacao.unique()])
        })
            group by 1
        """
        val = db.runQueryWithPandas(query)
        val['data_realizacao'] = pd.to_datetime(val['data_realizacao'])
        total_df['data_realizacao'] = pd.to_datetime(total_df['data_realizacao'])
        total_df = pd.merge(total_df,val,on='data_realizacao',how='left')

        print("Diferenças de mensalidades:")
        total_df['diferenca'] = total_df['faturamento'] - total_df['mens_total']
        total_df['prop%'] = total_df['diferenca'] / total_df['faturamento']
        total_df = total_df.loc[total_df['prop%'] > 0.001]
        display(total_df)

        if len(total_df) > 0:
            #por padrão corrige diferenca menor que 5%
            if not force:
                print('Corrigindo apenas diferenças de 5% ou menos...')
                print('Para corrigir todas as diferenças use a flag force=True')
                total_df = total_df.loc[total_df['prop%'] <= 0.05]
            else:
                print('Corrigindo todas diferenças...')

            print("Diferenças corrigidas:")
            display(total_df)

            total_df = total_df[['empresa_id', 'operadora', 'data_realizacao', 'diferenca']]
            total_df = total_df.rename(columns={'diferenca':'mensalidade'})
            total_df['rcode'] = '666'
            total_df['outros'] = 0.0
            df = pd.concat([df, total_df], axis=0, ignore_index=True)
            df['data_realizacao'] = pd.to_datetime(df['data_realizacao'])
        else:
            print("Nenhuma diferença de mensalidade elegível para correção")

        return df

def date_identifier(df, cols_nasc = 'nascimento'):


    if len(df[~df[f"{cols_nasc}"].isna()].reset_index(drop = True)[f"{cols_nasc}"][0].replace('/', '').replace(' 00:00:00', '')) == 6:

        var = df[~df[f"{cols_nasc}"].isna()][f"{cols_nasc}"].apply(lambda nums: (nums.replace('/', '').replace(' 00:00:00', '')[0:2] + ',' + nums.replace('/', '').replace(' 00:00:00', '')[2:4] + ',' + nums.replace('/', '').replace(' 00:00:00', '')[4:6]).split(',') if len(nums.replace('/', '').replace(' 00:00:00', '')) == 6 else '01,01,01'.split(','))
        #var = df[~df[f"{cols_nasc}"].isna()][f"{cols_nasc}"].apply(lambda x: x.split('/'))

        a = max([x[0] for x in var])
        b = max([x[1] for x in var])
        c = max([x[2] for x in var])

        if (a == b) or (a == c) or (b == c):
            print("          \x1b[38;5;168m\u2715 {} {}\x1b[0m".format('Algo de errado não está certo! Padrão de data alterna entre linhas, não é possível determinar para ', cols_nasc))
            x = 'erro'
            return x

        else:
            dict_vars = {'a': a, 'b': b, 'c': c}
            dict_temp = {'a': [], 'b': [], 'c': []}

            for var in dict_vars.keys():
                if (int(dict_vars[var]) == 0) or (int(dict_vars[var]) > 31):
                    dict_temp[var].append('y')

                elif (int(dict_vars[var]) > 0) and (int(dict_vars[var]) <= 31):
                    dict_temp[var].append('d')

                    if (int(dict_vars[var]) > 0) and (int(dict_vars[var]) <= 99):
                        if (int(dict_vars[var]) > 12):
                            dict_temp[var].append('y')

                        elif int(dict_vars[var]) <= 12:
                            dict_temp[var].append('m')

            if (len(dict_temp['a']) == len(dict_temp['b']) and len(dict_temp['a']) == len(dict_temp['c']) and len(dict_temp['b']) == len(dict_temp['c'])) and (dict_temp['a'] == dict_temp['b'] or dict_temp['a'] == dict_temp['c']):
                print("          \x1b[38;5;168m\u2715 {} \x1b[38;5;160m\033[01m{}\x1b[0m".format('Não foi possível determinar para', cols_nasc))
                x = 'erro'
                return x

            elif dict_temp['a'] == dict_temp['b'] or dict_temp['a'] == dict_temp['c']:
                print("          \x1b[38;5;168m\u2715 {} \x1b[38;5;160m\033[01m{}\x1b[0m".format('Não foi possível determinar para', cols_nasc))
                x = 'erro'
                return x

            elif len(dict_temp['a']) == 2 and len(dict_temp['b']) == 2 and len(dict_temp['c']) == 2 and dict_temp['a'] != dict_temp['b'] and dict_temp['a'] != dict_temp['c'] and dict_temp['b'] != dict_temp['c']:
                print("          \x1b[38;5;168m\u2715 {} \x1b[0m".format('Não há a mínima possibilidade de tal evento ocorrer!'))
                print(cols_nasc)
                x = 'erro'
                return x

            else:
                while True:
                    if  len(dict_temp['a']) == 1 and \
                            len(dict_temp['b']) == 1 and \
                            len(dict_temp['c']) == 1 and \
                            dict_temp['a'] != dict_temp['b'] and \
                            dict_temp['a'] != dict_temp['c'] and \
                            dict_temp['b'] != dict_temp['c']:


                        print("                         \x1b[38;5;2m\u2713 Identificado para \x1b[38;5;46m\033[01m{} \x1b[38;5;2m\u2794 %{}%{}%{} \x1b[0m".format(cols_nasc, list(dict_temp.values())[0][0], list(dict_temp.values())[1][0], list(dict_temp.values())[2][0]))
                        x = '%{}%{}%{}'.format(list(dict_temp.values())[0][0], list(dict_temp.values())[1][0], list(dict_temp.values())[2][0])


                        break


                    if len(dict_temp['a']) == 1 and (dict_temp['a'][0] in dict_temp['b'] or dict_temp['a'][0] in dict_temp['c']):
                        if len(dict_temp['b']) > 1 and dict_temp['a'][0] in dict_temp['b']:
                            dict_temp['b'] = list(set(dict_temp['b']) - set(dict_temp['a']))
                            pass

                        if len(dict_temp['c']) > 1 and dict_temp['a'][0] in dict_temp['c']:
                            dict_temp['c'] = list(set(dict_temp['c']) - set(dict_temp['a']))
                            pass

                        else:
                            pass


                    if len(dict_temp['b']) == 1 and (dict_temp['b'][0] in dict_temp['a'] or dict_temp['b'][0] in dict_temp['c']):
                        if len(dict_temp['a']) > 1 and dict_temp['b'][0] in dict_temp['a']:
                            dict_temp['a'] = list(set(dict_temp['a']) - set(dict_temp['b']))
                            pass

                        if len(dict_temp['c']) > 1 and dict_temp['b'][0] in dict_temp['c']:
                            dict_temp['c'] = list(set(dict_temp['c']) - set(dict_temp['b']))
                            pass

                        else: pass


                    if len(dict_temp['c']) == 1 and (dict_temp['c'][0] in dict_temp['a'] or dict_temp['c'][0] in dict_temp['b']):
                        if len(dict_temp['a']) > 1 and dict_temp['c'][0] in dict_temp['a']:
                            dict_temp['a'] = list(set(dict_temp['a']) - set(dict_temp['c']))
                            pass

                        if len(dict_temp['b']) > 1 and dict_temp['c'][0] in dict_temp['b']:
                            dict_temp['b'] = list(set(dict_temp['b']) - set(dict_temp['c']))
                            pass

                        else: pass

            return x

    #----------
    # 8 Dígitos
    #----------
    if len(df[~df[f"{cols_nasc}"].isna()].reset_index(drop = True)[f"{cols_nasc}"][0].replace('/', '').replace(' 00:00:00', '')) == 8:

        var = df[~df[f"{cols_nasc}"].isna()][f"{cols_nasc}"].apply(lambda nums: (nums.replace('/', '')[0:2] + ',' + nums.replace('/', '')[2:4] + ',' + nums.replace('/', '')[4:6] + ',' + nums.replace('/', '')[6:8]).split(',') if len(nums.replace('/', '').replace(' 00:00:00', '')) == 8 else '01,01,01,01'.split(','))

        a = max([x[0] for x in var])
        b = max([x[1] for x in var])
        c = max([x[2] for x in var])
        d = max([x[3] for x in var])


        if (a == b) or (a == c) or (a == d) or (b == c) or (b == d) or (c == d):
            print("          \x1b[38;5;168m\u2715 {} \x1b[38;5;160m\033[01m{}\x1b[0m".format('Não foi possível determinar para', cols_nasc))
            x = 'erro'
            return x
        else:
            dict_vars = {'a': a, 'b': b, 'c': c, 'd': d}
            dict_temp = {'a': [], 'b': [], 'c': [], 'd': []}

            for var in dict_vars.keys():
                if (int(dict_vars[var]) == 99) or (int(dict_vars[var]) == 0) or (int(dict_vars[var]) > 31):
                    dict_temp[var].append('y_2')
                    dict_temp[list(dict_temp.keys())[(list(dict_temp.keys()).index(var) - 1)]].append('Y')

                elif (int(dict_vars[var]) > 0) and (int(dict_vars[var]) <= 31):
                    dict_temp[var].append('d')

                    if int(dict_vars[var]) <= 12:
                        dict_temp[var].append('m')

            #(REVISAR) LÓGICA, POIS AGORA TEMOS 4 POSSIBILIDADES
            #--------------------------------------------------------------------------------------------------------------------------------------------
            if (len(dict_temp['a']) == len(dict_temp['b']) and len(dict_temp['a']) == len(dict_temp['c']) and len(dict_temp['b']) == len(dict_temp['c'])) and (dict_temp['a'] == dict_temp['b'] or dict_temp['a'] == dict_temp['c']):
                print("          \x1b[38;5;168m\u2715 {} \x1b[38;5;160m\033[01m{}\x1b[0m".format('Não foi possível determinar para', cols_nasc))
                x = 'erro'
                return x


            elif dict_temp['a'] == dict_temp['b'] or dict_temp['a'] == dict_temp['c']:
                print("         \x1b[38;5;168m\u2715 {} \x1b[38;5;160m\033[01m{}\x1b[0m".format('Não foi possível determinar para', cols_nasc))
                x= 'erro'
                return x


            elif len(dict_temp['a']) == 2 and len(dict_temp['b']) == 2 and len(dict_temp['c']) == 2 and dict_temp['a'] != dict_temp['b'] and dict_temp['a'] != dict_temp['c'] and dict_temp['b'] != dict_temp['c']:
                print('Não há a mínima possibilidade de tal evento ocorrer!')
                print(cols_nasc)
                x= 'erro'
                return x
            #--------------------------------------------------------------------------------------------------------------------------------------------

            else:

                while True:
                    if      len(dict_temp['a']) == 1 and \
                            len(dict_temp['b']) == 1 and \
                            len(dict_temp['c']) == 1 and \
                            len(dict_temp['d']) == 1 and \
                            dict_temp['a'] != dict_temp['b'] and \
                            dict_temp['a'] != dict_temp['c'] and \
                            dict_temp['a'] != dict_temp['d'] and \
                            dict_temp['b'] != dict_temp['c'] and \
                            dict_temp['b'] != dict_temp['d'] and \
                            dict_temp['c'] != dict_temp['d']:

                        final_format = []
                        for i in dict_temp.values():
                            if i[0] != 'y_2':
                                final_format.append(i)
                            else: pass

                        print("                         \x1b[38;5;2m\u2713 Identificado para \x1b[38;5;46m\033[01m{} \x1b[38;5;2m\u2794 %{}%{}%{} \x1b[0m".format(cols_nasc, final_format[0][0], final_format[1][0], final_format[2][0]))
                        x = '%{}%{}%{}'.format(final_format[0][0], final_format[1][0], final_format[2][0])

                        break


                    if len(dict_temp['a']) == 1:
                        if len(dict_temp['b']) > 1 and dict_temp['a'][0] in dict_temp['b']:
                            dict_temp['b'] = list(set(dict_temp['b']) - set(dict_temp['a']))
                            pass

                        if len(dict_temp['c']) > 1 and dict_temp['a'][0] in dict_temp['c']:
                            dict_temp['c'] = list(set(dict_temp['c']) - set(dict_temp['a']))
                            pass

                        if len(dict_temp['d']) > 1 and dict_temp['a'][0] in dict_temp['d']:
                            dict_temp['d'] = list(set(dict_temp['d']) - set(dict_temp['a']))
                            pass

                        else: pass


                    if len(dict_temp['b']) == 1:
                        if len(dict_temp['a']) > 1 and dict_temp['b'][0] in dict_temp['a']:
                            dict_temp['a'] = list(set(dict_temp['a']) - set(dict_temp['b']))
                            pass

                        elif len(dict_temp['c']) > 1 and dict_temp['b'][0] in dict_temp['c']:
                            dict_temp['c'] = list(set(dict_temp['c']) - set(dict_temp['b']))
                            pass

                        elif len(dict_temp['d']) > 1 and dict_temp['b'][0] in dict_temp['d']:
                            dict_temp['d'] = list(set(dict_temp['d']) - set(dict_temp['b']))
                            pass

                        else: pass


                    if len(dict_temp['c']) == 1:
                        if len(dict_temp['a']) > 1 and dict_temp['c'][0] in dict_temp['a']:
                            dict_temp['a'] = list(set(dict_temp['a']) - set(dict_temp['c']))
                            pass

                        elif len(dict_temp['b']) > 1 and dict_temp['c'][0] in dict_temp['b']:
                            dict_temp['b'] = list(set(dict_temp['b']) - set(dict_temp['c']))
                            pass

                        elif len(dict_temp['d']) > 1 and dict_temp['c'][0] in dict_temp['d']:
                            dict_temp['d'] = list(set(dict_temp['d']) - set(dict_temp['c']))
                            pass

                        else: pass


                    if len(dict_temp['d']) == 1:
                        if len(dict_temp['a']) > 1 and dict_temp['d'][0] in dict_temp['a']:
                            dict_temp['a'] = list(set(dict_temp['a']) - set(dict_temp['d']))
                            pass

                        elif len(dict_temp['b']) > 1 and dict_temp['d'][0] in dict_temp['b']:
                            dict_temp['b'] = list(set(dict_temp['b']) - set(dict_temp['d']))
                            pass

                        elif len(dict_temp['c']) > 1 and dict_temp['d'][0] in dict_temp['c']:
                            dict_temp['c'] = list(set(dict_temp['c']) - set(dict_temp['d']))
                            pass

                        else: pass

            return x
    else:
        print('Não suportado')
        x = 'erro'
        return x

def last_resort_date_identifier(df,coluna, **kwargs):
    df.reset_index(drop=True, inplace = True)
    if len(df[f"{coluna}"][0].replace('/', '').replace(' 00:00:00', '')) == 8:
        try:
            pd.to_datetime(df[f"{coluna}"].str.replace('/', '').replace(' 00:00:00', ''), format='%Y%m%d')
            return '%Y%m%d'

        except ValueError:
            try:
                pd.to_datetime(df[f"{coluna}"].str.replace('/', '').replace(' 00:00:00', ''), format='%d%m%Y')
                return '%d%m%Y'

            except ValueError:
                return 'Deu muito ruim'
    elif len(df[f"{coluna}"][0].replace('/', '').replace(' 00:00:00', '')) == 6:
        try:
            pd.to_datetime(df[f"{coluna}"].str.replace('/', '').replace(' 00:00:00', ''), format='%Y%m%d')
            return '%Y%m%d'

        except ValueError:
            try:
                pd.to_datetime(df[f"{coluna}"].str.replace('/', '').replace(' 00:00:00', ''), format='%d%m%y')
                return '%d%m%y'

            except ValueError:
                return 'Deu muito ruim'
    else: return 'Deu muito ruim'

def parse_date_from_date_identifier(df, **kwargs):
    from datetime import datetime

    def apply_date_identifier(data, date_format, **kwargs):
        if pd.isnull(data):
            return data

        data = data.replace('/', '').replace(' 00:00:00', '')

        if data in ['000000', '00000000']:
            return pd.to_datetime(np.nan)

        elif date_format == 'erro':
            return data

        elif len(data) in [6,8]:
            return datetime.strptime(data, date_format)

        else:
            return pd.to_datetime(np.nan)

    for coluna in ['inclusao_plano_real', 'cancelamento_plano_real', 'nascimento_2', 'data_inicio', 'data_pagamento', 'nascimento', 'data_de_nascimento']:
        if coluna in df.columns:
            df[coluna] = df[coluna].astype(str)
            print("\n\x1b[38;5;191m\u2794 Rodando parse_date_from_date_identifier em \x1b[38;5;43m\033[01m{} \x1b[0m".format(coluna))

            if (coluna == 'inclusao_plano_real' or coluna == 'cancelamento_plano_real') and 'nascimento_2' in df.columns:

                if len(df[f"{coluna}"][0].replace('/', '').replace(' 00:00:00', '')) == 8:
                    date_format = date_identifier(df, 'nascimento_2')
                    if date_format == 'erro':
                        print("    \x1b[38;5;191m\u2794 {} {}\x1b[0m".format('Rodando last_resort_date_identifier em', coluna))
                        x = last_resort_date_identifier(df, coluna=coluna)
                        #display(df)
                        if x == 'Deu muito ruim':
                            date_format = 'erro'
                            print("         \x1b[38;5;168m\u2715 {}\x1b[0m".format('Sem sucesso!'))
                        else:
                            print("          \x1b[38;5;2m\u2713 {} \x1b[38;5;43m\033[01m{}\x1b[0m".format('Sucesso: ', x))
                            date_format = x

                    df[f"{coluna}"] = df[f"{coluna}"].apply(lambda data: apply_date_identifier(data=data, date_format=date_format, coluna=coluna))

                    if date_format != 'erro' and coluna in ['nascimento', 'data_de_nascimento']:
                        df[f"{coluna}"] = df[f"{coluna}"].apply(lambda x: datetime.strptime('19' + str(x.year)[2:4] + str(x.month) + str(x.day), '%Y%m%d') if (datetime.now().year - x.year) < 0 else x)

                elif len(df[f"{coluna}"][0].replace('/', '').replace(' 00:00:00', '')) == 6:
                    date_format = date_identifier(df, 'nascimento')
                    if date_format == 'erro':
                        print("    \x1b[38;5;191m\u2794 {} {}\x1b[0m".format('Rodando last_resort_date_identifier em', coluna))
                        x = last_resort_date_identifier(df, coluna=coluna)
                        #display(df)
                        if x == 'Deu muito ruim':
                            date_format = 'erro'
                            print("         \x1b[38;5;168m\u2715 {}\x1b[0m".format('Sem sucesso!'))
                        else:
                            print("          \x1b[38;5;2m\u2713 {} \x1b[38;5;43m\033[01m{}\x1b[0m".format('Sucesso: ', x))
                            date_format = x

                    df[f"{coluna}"] = df[f"{coluna}"].apply(lambda data: apply_date_identifier(data=data, date_format=date_format, coluna=coluna))

                    if date_format != 'erro' and coluna in ['nascimento', 'data_de_nascimento']:
                        df[f"{coluna}"] = df[f"{coluna}"].apply(lambda x: datetime.strptime('19' + str(x.year)[2:4] + str(x.month) + str(x.day), '%Y%m%d') if (datetime.now().year - x.year) < 0 else x)

                else:
                    pass

            # Para Eventos
            # Identifica o formato de nascimento e propaga ele para data_pagamento e data_inicio
            elif coluna in ['data_inicio', 'data_pagamento'] and 'nascimento' in df.columns:
                if len(max(df[f'{coluna}']).replace('/', '').replace(' 00:00:00', '')) == len(max(df['nascimento']).replace('/', '').replace(' 00:00:00', '')):
                    date_format = date_identifier(df, 'nascimento')

                    if date_format == 'erro':
                        print("    \x1b[38;5;191m\u2794 {} {}\x1b[0m".format('Rodando last_resort_date_identifier em', coluna))
                        x = last_resort_date_identifier(df, coluna=coluna)
                        #display(df)
                        if x == 'Deu muito ruim':
                            date_format = 'erro'
                            print("         \x1b[38;5;168m\u2715 {}\x1b[0m".format('Sem sucesso!'))
                        else:
                            print("          \x1b[38;5;2m\u2713 {} \x1b[38;5;43m\033[01m{}\x1b[0m".format('Sucesso: ', x))
                            date_format = x

                    df[f"{coluna}"] = df[f"{coluna}"].apply(lambda data: apply_date_identifier(data=data, date_format=date_format, coluna=coluna))

                    if date_format != 'erro' and coluna in ['nascimento', 'data_de_nascimento']:
                        df[f"{coluna}"] = df[f"{coluna}"].apply(lambda x: datetime.strptime('19' + str(x.year)[2:4] + str(x.month) + str(x.day), '%Y%m%d') if (datetime.now().year - x.year) < 0 else x)

            else:

                date_format = date_identifier(df, coluna)

                if date_format == 'erro':
                    print("    \x1b[38;5;191m\u2794 {} {}\x1b[0m".format('Rodando last_resort_date_identifier em', coluna))
                    x = last_resort_date_identifier(df, coluna=coluna)
                    #display(df)
                    if x == 'Deu muito ruim':
                        date_format = 'erro'
                        print("         \x1b[38;5;168m\u2715 {}\x1b[0m".format('Sem sucesso!'))
                    else:
                        print("          \x1b[38;5;2m\u2713 {} \x1b[38;5;43m\033[01m{}\x1b[0m".format('Sucesso: ', x))
                        date_format = x

                df[f"{coluna}"] = df[f"{coluna}"].apply(lambda data: apply_date_identifier(data=data, date_format=date_format, coluna=coluna))

                if date_format != 'erro' and coluna in ['nascimento', 'data_de_nascimento']:
                    df[f"{coluna}"] = df[f"{coluna}"].apply(lambda x: datetime.strptime('19' + str(x.year)[2:4] + str(x.month) + str(x.day), '%Y%m%d') if (datetime.now().year - x.year) < 0 else x)

        else:
            pass

    if ('nascimento' in df.columns or 'data_de_nascimento' in df.columns) and 'nascimento_2' in df.columns:
        print('excluindo nascimento_2 ou Y2K')

        if 'nascimento' in df.columns:
            df['nascimento'] = df['nascimento_2']
            del df['nascimento_2']
        elif 'data_nascimento' in df.columns:
            df['data_de_nascimento'] = df['nascimento_2']
            del df['nascimento_2']
        else: pass

    return df

def fix_matricula_familia(df, **kwargs):
    if 'matricula_familia' in df.columns:
        df['matricula_familia'] = df['matricula_familia'].astype(int)
        return df
    else:
        print('Não há coluna matricula_familia')
        return df

def ben_validador_check(result, db):
    """Retorna um comparativo de vidas (MTM) com o validador!"""

    merged = db.runQueryWithPandas(f"SELECT empresa_id, data, vidas FROM BI_Validador WHERE empresa_id = {result.empresa_id.unique()[0]} AND operadora LIKE '%%{result.operadora.unique()[0]}%%' AND data >= '{str(pd.to_datetime(min(result.competencia.unique()))).replace(' 00:00:00', '')}' AND data <= '{str(pd.to_datetime(max(result.competencia.unique()))).replace(' 00:00:00', '')}'")
    merged['vidas'].fillna(0, inplace = True)

    if merged.empty:
        print('Sem validador para empresa_id = {}'.format(result.empresa_id.unique()[0]))
        return result[["empresa_id", "competencia", "nome"]].groupby(by=["empresa_id", "competencia"]).nunique()
    else:
        print('Com validador para empresa_id = {}'.format(result.empresa_id.unique()[0]))
        result['competencia'] = pd.to_datetime(result['competencia'])

        merged = merged.groupby('data').sum().reset_index()
        temp = result[["empresa_id", "competencia", "nome"]].groupby(by=["empresa_id", "competencia"]).nunique().reset_index().sort_values('competencia').merge(merged, how ='left', left_on = 'competencia', right_on = 'data')

        temp['comparacao'] = temp['vidas'] - temp['nome']
        temp['dif'] = round((temp['vidas'] / temp['nome'] - 1) * 100,2)
        temp['dif'] = temp['dif'].apply(lambda x: str(x) + '%')

        temp.rename(columns = {'nome': 'vidas_arquivo', 'empresa_id_y': 'empresa_id', 'vidas': 'vidas_validador', 'dif': 'diferença'}, inplace=True)

        return temp[['empresa_id', 'competencia', 'vidas_arquivo', 'vidas_validador', 'diferença']].sort_values('competencia', ascending=False)

def check_with_validador_ben(result, db):
    """Aplica a função ben_validador_check e mostra o valor, suporte para múltiplas empresas!"""
    for empresa in result.empresa_id.unique():
        display(ben_validador_check(result[result['empresa_id'] == empresa], db))

def ev_validador_check(evs, db):
    """Retorna um comparativo de eventos com o Validador!"""
    merged = db.runQueryWithPandas(f"SELECT data, sinistro FROM BI_Validador WHERE empresa_id = {evs.empresa_id.unique()[0]} AND operadora LIKE '%%{evs.operadora.unique()[0]}%%' AND data >= '{str(pd.to_datetime(min(evs.competencia.unique()))).replace(' 00:00:00', '')}' AND data <= '{str(pd.to_datetime(max(evs.competencia.unique()))).replace(' 00:00:00', '')}'")
    if merged.empty:
        print('Sem validador para empresa_id = {}'.format(evs.empresa_id.unique()[0]))
        return evs[["empresa_id", "competencia", "valor"]].groupby(by=["empresa_id", "competencia"]).sum()
    else:
        import locale
        locale.setlocale( locale.LC_ALL, 'pt_BR.UTF-8' )
        print('Com validador para empresa_id = {}'.format(evs.empresa_id.unique()[0]))
        evs['competencia'] = pd.to_datetime(evs['competencia'])
        merged = merged.groupby('data').sum().reset_index()
        temp = evs[["empresa_id", "competencia", "valor"]].groupby(by=["empresa_id", "competencia"]).sum().reset_index().merge(merged, how = 'left', left_on = 'competencia', right_on = 'data')
        temp['sinistro'].fillna(0, inplace = True)

        if len(temp['sinistro'].unique()) == 1 and temp['sinistro'].unique()[0] == 0.0:
            print('Sem validador para sinistros para empresa_id = {}'.format(evs.empresa_id.unique()[0]))
            temp['valor'] = temp['valor'].apply(lambda x: locale.currency(x, grouping = True))
            return temp[['competencia', 'valor']]

        else:
            print('Com validador para sinsitros para empresa_id = {}'.format(evs.empresa_id.unique()[0]))
            temp['diferenca'] = round((temp['valor'] / temp['sinistro'] - 1)*100,3)
            temp['valor'] = temp['valor'].apply(lambda x: locale.currency(x, grouping = True))
            temp['sinistro'] = temp['sinistro'].apply(lambda x: locale.currency(x, grouping = True))
            temp['diferenca'] = temp['diferenca'].apply(lambda x: str(x) + '%')
            temp.rename(columns = {'empresa_id_x': 'empresa_id', 'valor': 'sinistro_arquivo', 'sinistro': 'sinistro_validador'}, inplace = True)
            return temp[['empresa_id', 'competencia', 'sinistro_arquivo', 'sinistro_validador', 'diferenca']]

def check_with_validador_evs(evs, db):
    """Aplica a função ben_validador_check e mostra o valor, suporte para múltiplas empresas!"""
    for empresa in evs.empresa_id.unique():
        display(ev_validador_check(evs[evs['empresa_id'] == empresa], db))


def apply_new_md5(df, **kwargs):
    def new_md5_in_python(name):
        """Nova função de MD5 conforme painel de teste. Agora em Python!"""
        from hashlib import md5
        #Encode está no padrão do MySQL
        encode = 'utf-8'

        #Função que irá aplicar o replace no texto automáticamente a partir de um dict
        def replace_all(text, replaces_dict):
            for i, j in replaces_dict.items():
                text = text.replace(i, j)
            return text

        # Dicionário dos replaces que desejamos usar.
        replaces_dict = {"e": " ",
                         "dos": " ",
                         "das": " ",
                         "do": " ",
                         "de": " ",
                         "da": " ",
                         " ": ""
                         }
        name = str(name)
        return md5(replace_all(name, replaces_dict).encode(encode)).hexdigest()


    for coluna in ['nome_beneficiario', 'nome', 'nome_titular']:
        if coluna in df.columns:
            if coluna in ['nome_beneficiario', 'nome']:
                df['cpf'] = df[f"{coluna}"].apply(lambda name: new_md5_in_python(name))

            else:
                df['cpf_titular'] = df[f'{coluna}'].apply(lambda name: new_md5_in_python(name))

    return df

def fix_vidas_ponto(df, **kwwargs):
    if 'vidas' in df.columns:
        df['vidas'] = df['vidas'].apply(lambda x: str(x).replace(".", ""))
    return df

class get_cards:
    """Informa os cards da empresa, sendo necessário informar apenas o df, esse sendo evs, bens, cols, results, mens ou cops, irá funcionar com todos!"""

    # 'OUTER CLASS'
    def __init__(self, df, db = None):
        # Definindo df
        self.df = df
        self.db = db

        # Definindo Inner Class
        self.get_print_cards = self.how_to_print(None)

    # Inner Class
    class how_to_print:
        """Imprime os resultados conforme o type informado!"""
        def __init__(self, type) -> str:
            self.types = str(type).lower()

        # Printa setas que ficam acima do texto com coloração baseada no type
        def upper_limit(self, range_limit):
            if self.types == 'warning':
                return "{}".format('\x1b[38;5;9m\033[4m\x1b[38;5;9m\u2193 \x1b[0m'*range_limit)
            if self.types == 'sql':
                return "{}".format('\x1b[38;5;9m\033[4m\x1b[38;5;104m\u2193 \x1b[0m'*range_limit)
            if self.types == 'comment':
                return "{}".format('\x1b[38;5;9m\033[4m\x1b[38;5;209m\u2193 \x1b[0m'*range_limit)
            else:
                return "{}".format('\x1b[38;5;9m\033[4m\x1b[38;5;42m\u2193 \x1b[0m'*range_limit)


        # Imprime linhas, com coloração baseada no type
        def lower_limit(self, range_limit):
            if self.types == 'warning':
                return "{}".format('\x1b[38;5;9m\x1b[38;5;9m__\x1b[0m'*range_limit)
            if self.types == 'sql':
                return "{}".format('\x1b[38;5;9m\x1b[38;5;104m__\x1b[0m'*range_limit)
            if self.types == 'comment':
                return "{}".format('\x1b[38;5;9m\x1b[38;5;209m__\x1b[0m'*range_limit)
            else:
                return "{}".format('\x1b[38;5;9m\x1b[38;5;42m__\x1b[0m'*range_limit)


        # Imprime as mensagems que temos no DB
        def return_message(self, campo_1, campo_2, empresa):
            if self.types == 'warning':
                return "\033[1m\x1b[38;5;9m{} \u2192 empresa_id = {}\n\x1b[0m \u23fa Data de criação em: {}\n \u23fa Criador: {}\n".format(self.types.upper(), empresa, campo_1, campo_2)
            if self.types == 'sql':
                return "\033[1m\x1b[38;5;104m{} \u2192 empresa_id = {}\n\x1b[0m \u23fa Data de criação em: {}\n \u23fa Criador: {}\n".format(self.types.upper(), empresa, campo_1, campo_2)
            if self.types == 'comment':
                return "\033[1m\x1b[38;5;209m{} \u2192 empresa_id = {}\n\x1b[0m \u23fa Data de criação em: {}\n \u23fa Criador: {}\n".format(self.types.upper(), empresa, campo_1, campo_2)
            else:
                return "\033[1m\x1b[38;5;42m{} \u2192 empresa_id = {}\n\x1b[0m \u23fa Data de criação em: {}\n \u23fa Criador: {}\n".format(self.types.upper(), empresa, campo_1, campo_2)

        # FIM DO INNER CLASS ----------------------------------------------------------------

    #Função que irá chamar as classes acima para printar os textos, gerando assim os cards
    def print_cards(self, db = None, operadora = None):

        # Verifica se temos a coluna necessária, caso não raise um ValueError
        if 'empresa_id' not in self.df.columns:
            raise ValueError("empresa_id columns missing")

        # Se tivermos a coluna segue para geração do card
        if 'empresa_id' in self.df.columns:

            # Abaixo realiza a análise para todas as empresas que podemos ter no DF informado
            for empresa in self.df['empresa_id'].unique():

                # Ainda não temos coluna de operadora no DB por isso forçamos ela ser None
                if operadora is not None:
                    print('Ainda sem suporte para exibir por operadora!')
                    operadora = None

                # Pegamos os dados do banco para preparar o card
                temp = db.runQueryWithPandas(f"""
                    SELECT *
                    FROM BI_Company_info
                    WHERE empresa_id = {str(empresa)}
                      {'' if operadora is None else "AND operadora LIKE '%" + str(operadora) + "%'"}
                    ORDER BY CASE
                                WHEN type = 'warning' THEN 1
                                WHEN type = 'comment'THEN 2
                                WHEN type = 'sql' THEN 3
                                ELSE 4 END
                    """)

                # Se não tivermos nada no DB retorna a mensagem informando que não temos o card disponível
                if temp.empty:
                    print(f'\u2192\x1b[31m Nenhum card encontrado para empresa_id = \x1b[0m{empresa}')

                # Se tivermos dados no DB o card será gerado e exibido ao usuário
                else:
                    for index in temp.index:
                        type = temp.loc[index, 'type']
                        print(get_cards.how_to_print(type = type).upper_limit(range_limit = (len(temp.loc[index, 'info']) if len(temp.loc[index, 'info']) <= 50 else 50)))
                        print(get_cards.how_to_print(type).return_message(empresa = empresa, campo_1 = temp.loc[index, 'data'].strftime("%Y-%m-%d"), campo_2 = temp.loc[index, 'user']))
                        print(temp.loc[index, 'info'])
                        print(get_cards.how_to_print(type = type).lower_limit(range_limit = (len(temp.loc[index, 'info']) if len(temp.loc[index, 'info']) <= 50 else 50)))
        # Se esse erro aparecer algo deu muito errado!
        else:
            raise Exception("Algum problema inesperado ocorreu!")

class cheeck_with_db:
    """Verifica os dados com o nosso DB"""
    def __init__(self, df):

        self.df = df

    def is_in_db(self, db, tabela = 'eventos'):

        """Informa de os dados no df informado já estão no DB da empresa.
        :param db: database
        :param tabela: eventos, result, mensalidade, copay"""

        self.db = db
        self.tabela = tabela

        if tabela not in ('eventos', 'mensalidade', 'copay', 'evs', 'result', 'cols', 'benes'):
            raise Exception(f"{tabela} Não está dentro do suporte para tabelas selecione: 'eventos', 'mensalidade', 'copay', 'evs', 'result', 'cols', 'benes' ")
        else:
            for empresa in self.df.empresa_id.unique():
                for comp in sorted(self.df[self.df['empresa_id'] == empresa]['data_realizacao' if self.tabela in ('copay', 'mensalidade', 'mens') else 'competencia'].unique()):
                    temp = db.runQueryWithPandas(f"""
                    SELECT DISTINCT {'data_realizacao' if self.tabela in ('copay', 'mensalidade', 'mens') else 'competencia'}
                    FROM {'BI_Eventos' if self.tabela in ('evs', 'eventos') else ('BI_Colaborador' if self.tabela in ('result', 'benes', 'cols') else ('BI_Mensalidades' if self.tabela in ('copay', 'mensalidade', 'mens') else 'X'))}
                    WHERE empresa_id = {empresa}
                      AND operadora LIKE '%%{self.df[self.df['empresa_id'] == empresa]['operadora'].unique()[0]}%%'
                      AND {'data_realizacao' if self.tabela in ('copay', 'mensalidade', 'mens') else 'competencia'} = '{pd.to_datetime(comp).strftime('%Y-%m-%d')}'
                      {f'AND {"valor" if self.tabela in ("evs", "eventos") else "mensalidade"} <> 0' if self.tabela not in ('result', 'benes', 'cols') else ''};""")
                    if temp.empty:
                        print(f"""OK para '{empresa}' e operadora '{self.df[self.df['empresa_id'] == empresa]['operadora'].unique()[0]}' em {pd.to_datetime(comp).strftime('%Y-%m-%d')}""")
                    else:
                        print(f"""Já temos dados para {empresa} em {pd.to_datetime(comp).strftime('%Y-%m-%d')}""")

def is_mtm_from_db(db=None, empresa_id=None, operadora=None, threshold=0.75):
    try:
        query = """
        select sum(if(t1.is_mtm = 1, pessoas, 0)) / sum(pessoas) as pessoas_mtm_prop
        from (select count(cpf)                                         pessoas,
                     if(inclusao_plano_real=inclusao_plano and day(inclusao_plano) = 1, 0, 1) as    is_mtm
            from BI_Colaborador
            where operadora like '%%{operadora}%%'
                and Empresa_ID = {empresa_id}
                and inclusao_plano is not null
                and cancelamento_plano is not null
                and inclusao_plano <> cancelamento_plano
                and rcode is null
            group by 2
            union all
            select count(cpf)                                         pessoas,
                   if(inclusao_plano_real=inclusao_plano and day(inclusao_plano) = 1, 0, 1) as    is_mtm
            from BI_Beneficiario
            where operadora like '%%{operadora}%%'
                and Empresa_ID = {empresa_id}
                and inclusao_plano is not null
                and cancelamento_plano is not null
                and inclusao_plano <> cancelamento_plano
                and rcode is null
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

    #Se retornar None, não tem dados no banco
    if pessoas_mtm_prop is None:
        return False

    #Se no banco mais de 75% dos dados estiverem por MTM retorna True
    if pessoas_mtm_prop is not None:
        if pessoas_mtm_prop >= threshold:
            return True

    #Se não for maior que 75% retorna False
    return False

def Aplicar_DEV_UUID(df, **kwargs):
    db = kwargs.get("db")
    verbose = kwargs.get("verbose", False)
    operadora = str(df['operadora'].unique()[0])
    if "uuid" not in df.columns:
        df['uuid'] = None
    for empresa_id in df['empresa_id'].unique():
        DEV_UUID = db.runQueryWithPandas(f"""SELECT * FROM DEV_UUID
                                                WHERE empresa_id = {empresa_id} 
                                                AND operadora LIKE concat('%%', '{operadora}', '%%');""")
        if DEV_UUID.empty == False:
            if "seguros unimed" in operadora.lower():
                df = ho.script_homonimo_para_SEGUROS_UNIMED(df, DEV_UUID)
            if "central nacional unimed" in operadora.lower():
                df = ho.script_homonimo_para_CNU(df, DEV_UUID)
            if "unimed guarulhos" in operadora.lower():
                df = ho.script_homonimo_para_UNIMED_GUARULHOS(df, DEV_UUID)
            if "hapvida" in operadora.lower():
                df = ho.script_homonimo_para_HAPVIDA(df, DEV_UUID)
            if "gndi" in operadora.lower():
                df = ho.script_homonimo_para_GNDI(df, DEV_UUID)
    return df

def Aplicar_DEV_Filtros(df, **kwargs):
    def rename_cols(df, coluna_origem, coluna_destino):
        if "nome_beneficiario" in df.columns or "mensalidade" in df.columns or "data_realizacao" in df.columns: #BI_Eventos e BI_Mensalidade
            if "plano" not in df.columns:
                df['plano'] = None
            if coluna_origem == 'nome_plano':
                coluna_origem = 'plano'
            if coluna_destino == 'nome_plano':
                coluna_destino = 'plano'
        elif "nome" in df.columns and "mensalidade" not in df.columns: #BI_Beneficiario e BI_Colaborador
            if "nome_plano" not in df.columns:
                df['nome_plano'] = None
            if coluna_origem == 'plano':
                coluna_origem = 'nome_plano'
            if coluna_destino == 'plano':
                coluna_destino = 'nome_plano'
        return coluna_origem, coluna_destino

    db = kwargs.get("db")
    verbose = kwargs.get("verbose", False)
    operadora = str(df['operadora'].unique()[0])

    lista_nao_contemplados = []
    contemplados = []
    for empresa_id in df['empresa_id'].unique():
        DEV_Filtros_Total = db.runQueryWithPandas(f"""SELECT * FROM DEV_Filtros
                                                WHERE empresa_id = {empresa_id} 
                                                AND operadora LIKE concat('%%', '{operadora}', '%%');""")
        DEV_Filtros_LIKE = DEV_Filtros_Total[DEV_Filtros_Total['De'].str.contains('^%.*%$', regex=True)]
        DEV_Filtros = DEV_Filtros_Total[~DEV_Filtros_Total['De'].str.contains('%')]

        if DEV_Filtros.empty == False:
            tupla_dev = DEV_Filtros[['coluna_origem', 'coluna_destino']]
            tupla_dev.drop_duplicates(inplace=True)
            for i in range(0,len(tupla_dev),1):
                coluna_origem = tupla_dev['coluna_origem'].iloc[i]
                coluna_destino = tupla_dev['coluna_destino'].iloc[i]
                escolher_DEV_Filtros = DEV_Filtros[DEV_Filtros['coluna_destino']==coluna_destino]
                coluna_origem, coluna_destino = rename_cols(df, coluna_origem, coluna_destino)
                if coluna_origem in df.columns and coluna_destino in df.columns:
                    df[coluna_destino] = df[coluna_destino].astype(str)
                    df[coluna_origem] = df[coluna_origem].astype(str)
                    for j in range(0, len(escolher_DEV_Filtros), 1):
                        df[coluna_destino] = df[coluna_destino].mask(df[coluna_origem].str.lower() == escolher_DEV_Filtros['De'].iloc[j].lower(), escolher_DEV_Filtros['Para'].iloc[j].lower())
            for i in DEV_Filtros['coluna_destino'].unique():
                coluna_origem, i = rename_cols(df, coluna_origem, i)
                if i in df.columns:
                    for j in df[i].unique():
                        if j in DEV_Filtros['Para'].unique() or j in DEV_Filtros_LIKE['Para'].unique():
                            if j not in contemplados:
                                contemplados.append(j)
                                print("\x1b[38;5;2m\u2713 {}: \x1b[38;5;43m\033[01m{}\x1b[0m".format(i, j))
                        else:
                            tupla_i_j = (i, j)
                            lista_nao_contemplados.append(tupla_i_j)
        if DEV_Filtros_LIKE.empty == False:
            print("\x1b[38;5;5m\u2713 {}: \x1b[38;5;5m\033[152m{}\x1b[0m".format('LIKE', DEV_Filtros_LIKE['De'].unique()))
            tupla_dev = DEV_Filtros_LIKE[['coluna_origem', 'coluna_destino']]
            tupla_dev.drop_duplicates(inplace=True)
            for i in range(0,len(tupla_dev),1):
                coluna_origem = tupla_dev['coluna_origem'].iloc[i]
                coluna_destino = tupla_dev['coluna_destino'].iloc[i]
                escolher_DEV_Filtros = DEV_Filtros_LIKE[DEV_Filtros_LIKE['coluna_destino']==coluna_destino]
                coluna_origem, coluna_destino = rename_cols(df, coluna_origem, coluna_destino)
                if coluna_origem in df.columns and coluna_destino in df.columns:
                    df[coluna_destino] = df[coluna_destino].astype(str)
                    df[coluna_origem] = df[coluna_origem].astype(str)
                    for j in range(0, len(escolher_DEV_Filtros), 1):
                        escolhido = escolher_DEV_Filtros['De'].iloc[j].replace('%','').lower()
                        df[coluna_destino] = df[coluna_destino].mask(df[coluna_origem].str.lower().str.contains(escolhido), escolher_DEV_Filtros['Para'].iloc[j].lower())
            for i in DEV_Filtros_LIKE['coluna_destino'].unique():
                coluna_origem, i = rename_cols(df, coluna_origem, i)
                if i in df.columns:
                    for j in df[i].unique():
                        if j in DEV_Filtros_LIKE['Para'].unique() or j in DEV_Filtros['Para'].unique():
                            if j not in contemplados:
                                contemplados.append(j)
                                print("\x1b[38;5;2m\u2713 {}: \x1b[38;5;43m\033[01m{}\x1b[0m".format(i, j))
                        else:
                            tupla_i_j = (i, j)
                            lista_nao_contemplados.append(tupla_i_j)
        for i in lista_nao_contemplados:
            coluna_origem = i[0]
            valor = i[1]
            if coluna_origem in df.columns:
                if valor in df[coluna_origem].unique():
                    print("\x1b[38;5;1m\u2715 {}: \x1b[338;5;1m\033[01m{}\x1b[0m".format(coluna_origem, valor))

        if DEV_Filtros_LIKE.empty == True and DEV_Filtros.empty == True:
            print("\x1b[38;5;168m\u2715 {}\x1b[0m".format('!!! --> FILTROS NAO ADICIONADOS NO DEV_FILTROS <-- !!!'))

    return df

def parse_not_be_null(df, **kwargs):
    columns = ["outros"]
    for column in columns:
        if column in df.columns:
            df[column] = df[column].fillna(0.0)
    return df


def is_mtm_from_db(db=None, empresa_id=None, operadora=None, threshold=0.75):
    try:
        query = """
        select sum(if(t1.is_mtm = 1, pessoas, 0)) / sum(pessoas) as pessoas_mtm_prop
        from (select count(cpf)                                         pessoas,
                     if(inclusao_plano_real=inclusao_plano and day(inclusao_plano) = 1, 0, 1) as    is_mtm
            from BI_Colaborador
            where operadora like '%%{operadora}%%'
                and Empresa_ID = {empresa_id}
                and inclusao_plano is not null
                and cancelamento_plano is not null
                and inclusao_plano <> cancelamento_plano
                and rcode is null
            group by 2
            union all
            select count(cpf)                                         pessoas,
                   if(inclusao_plano_real=inclusao_plano and day(inclusao_plano) = 1, 0, 1) as    is_mtm
            from BI_Beneficiario
            where operadora like '%%%{operadora}%%%'
                and Empresa_ID = {empresa_id}
                and inclusao_plano is not null
                and cancelamento_plano is not null
                and inclusao_plano <> cancelamento_plano
                and rcode is null
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

    #Se retornar None, não tem dados no banco
    if pessoas_mtm_prop is None:
        return False

    #Se no banco mais de 75% dos dados estiverem por MTM retorna True
    if pessoas_mtm_prop is not None:
        if pessoas_mtm_prop >= threshold:
            return True

    #Se não for maior que 75% retorna False
    return False


def Aplicar_Tabela_Mae(df=None, db=None, segmentar_exames=False, **kwargs):
    def processar_str(x):
        if pd.isnull(x):
            return x
        else:
            return decode_string(str(x)).upper()

    if 'codigo_procedimento' not in df.columns and 'descricao' not in df.columns:
        print('Erro de categorização, verifique se as colunas codigo_procedimento e descricao existem no DataFrame')
        return df

    if 'categoria' not in df.columns:
        df['categoria'] = np.nan

    if len(df.loc[df.categoria.isna()]) == 0:
        print('Nenhum registro sem categoria retornando DataFrame')
        return df

    #Aux para nao sobrescrever as originais
    df['_descricao'] = df['descricao'].apply(processar_str)
    df['_codigo_procedimento'] = df['codigo_procedimento'].apply(processar_str)

    codigos = df.loc[df.categoria.isna()]._codigo_procedimento.unique()
    tab_mae = db.runQueryWithPandas(f"""
        select  distinct codigo_procedimento as _codigo_procedimento,
                descricao as _descricao,
                {'tipo_exame' if segmentar_exames else 'categoria'} as _categoria
        from DEV_Tabela_Mae
        where { 'tipo_exame' if segmentar_exames else 'categoria'} is not null
        and codigo_procedimento in ({','.join(["'"+str(codigo)+"'" for codigo in codigos])})
        """)



    tab_mae['_descricao'] = tab_mae['_descricao'].apply(processar_str)
    # tab_mae['_descricao'] = tab_mae['_descricao'].apply(decode_string).apply(str.upper)
    # tab_mae['_codigo_procedimento'] = tab_mae['_codigo_procedimento'].apply(decode_string)
    tab_mae['_codigo_procedimento'] = tab_mae['_codigo_procedimento'].apply(processar_str)
    tab_mae = tab_mae.drop_duplicates(subset=['_descricao', '_codigo_procedimento'])

    # print(len(df))
    df = pd.merge(
        df,
        tab_mae,
        how="left",
        on=['_descricao', '_codigo_procedimento'],
    )
    # print(len(df))

    #Combine first para nao sobrescrever passos rodados anteriormente na coluna categoria
    df['categoria'] = df['categoria'].combine_first(df['_categoria'])

    del df['_descricao']
    del df['_codigo_procedimento']
    del df['_categoria']

    return df


# adicionar nos parser das operadoras
def lower_names_filters(df, **kwargs):
    # função para passar colunas de filtros para lower
    debug = kwargs.get('debug', False)
    filtros = ['operadora', 'subestipulante', 'nome_plano', 'plano', 'status', 'sexo', 'titularidade', 'aux','lotacao','categoria']

    for column in filtros:
        if column in df.columns:
            df[column] = df[column].apply(lambda x: str(x).lower())
    return df
