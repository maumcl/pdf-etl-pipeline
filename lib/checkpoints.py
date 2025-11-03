import pandas as pd
import re
from lib.legado.WellbeTest import *

### Verificação de colunas (simplesmente verifica se estão ou não no df)

colunas_obrigatorias = ["empresa_id", "operadora", "fcode"]
colunas_gerais = ["sexo", "titularidade", "status", "subestipulante", "cpf", "cpf_real"]

colunas_ben_1 = ["nome", "inclusao_plano"]
colunas_ben_2 = ["nome_titular", "cpf_titular", "cancelamento_plano",
               "competencia", "inclusao_plano_real", "cancelamento_plano_real",
               "cpf_real_titular", "nome_plano"]

colunas_ev_1 = ["codigo_procedimento", "categoria", "valor", "competencia", "nome_beneficiario"]
colunas_ev_2 = ["plano", "executor", "especialidade", "data_inicio", "guia", "descricao"]

colunas_men_1 = ["mensalidade", "data_realizacao"]
colunas_cop_1 = ["coparticipacao", "data_realizacao"]


def verificar_colunas(df,
                      test_columns,
                      not_in=True,
                      critical=False,
                      show_success=False):
    success_flag = True
    for test_column in test_columns:
        if (test_column not in df.columns) == not_in:
            success_flag = False
            if critical:
                error(f'verificar_colunas: coluna importante "{test_column}" não está no df\n')
            else:
                warning(f'verificar_colunas: coluna "{test_column}" não está no df\n')
        elif show_success:
            success(f'verificar_colunas: coluna "{test_column}" está no df')
    if success_flag:
        if critical:
            success('verificar_colunas: todas as colunas obrigatórias estão no df')
        else:
            success('verificar_colunas: todas as colunas desejáveis estão no df')
###

### funções orquestradoras (chamam as demais)
def checkpoint_ben(cols, benes):
    print("Verificando cols")
    verificar_colunas(cols, colunas_obrigatorias + colunas_ben_1, critical=True)
    verificar_colunas(cols, colunas_gerais + colunas_ben_2 + ["data_de_nascimento"], critical=False)
    test_nomes(cols)
    test_sexo(cols)
    test_status(cols)
    test_titularidade(cols)
    test_datas(cols, ["data_de_nascimento", "inclusao_plano", "inclusao_plano_real", "competencia"])
    test_datas(cols, ["cancelamento_plano", "cancelamento_plano_real"], verify_nulls=False)
    test_codigos(cols, ["cpf_real", "cod_plano", "cpf_real_titular", "matricula_familia"])
    test_nao_ativos(cols)
    print("Verificando benes")
    verificar_colunas(benes, colunas_obrigatorias + colunas_ben_1, critical=True)
    verificar_colunas(benes, colunas_gerais + colunas_ben_2 + ["nascimento"], critical=False)
    test_nomes(benes)
    test_sexo(benes)
    test_status(benes)
    test_titularidade(benes)
    test_datas(benes, ["nascimento", "inclusao_plano", "inclusao_plano_real", "competencia"])
    test_datas(cols, ["cancelamento_plano", "cancelamento_plano_real"], verify_nulls=False)
    test_codigos(benes, ["cpf_real", "cod_plano", "cpf_real_titular", "matricula_familia"])
    test_nao_ativos(benes)


def checkpoint_ev(evs):
    evs = test_specifics(evs)
    verificar_colunas(evs, colunas_obrigatorias + colunas_ev_1, critical=True)
    verificar_colunas(evs, colunas_gerais + colunas_ev_2, critical=False)
    test_nomes(evs)
    test_sexo(evs)
    test_status(evs)
    test_titularidade(evs)
    test_float(evs, ["valor", "valor_apresentado", "valor_total", "coparticipacao"])
    test_datas(evs, ["competencia", "data_inicio", "data_pagamento"])
    test_datas(evs, ["inicio_internacao", "alta_internacao"], verify_nulls=False)
    test_codigos(evs, ["codigo_procedimento", "cpf_real", "cod_plano", "cpf_real_titular", "matricula_familia"])


def checkpoint_men(mens):
    verificar_colunas(mens, colunas_obrigatorias + colunas_men_1, critical=True)
    verificar_colunas(mens, colunas_gerais, critical=False)
    test_nomes(mens)
    test_sexo(mens)
    test_status(mens)
    test_titularidade(mens)
    test_float(mens, ["coparticipacao", "mensalidade", "outros"])
    test_datas(mens, ["data_realizacao"])
    test_codigos(mens, ["cpf_real", "cpf_real_titular"])


def checkpoint_cop(cops):
    verificar_colunas(cops, colunas_obrigatorias + colunas_cop_1, critical=True)
    verificar_colunas(cops, colunas_gerais, critical=False)
    test_sexo(cops)
    test_status(cops)
    test_titularidade(cops)
    test_float(cops, ["coparticipacao", "mensalidade", "outros"])
    test_datas(cops, ["data_realizacao"])
    test_codigos(cops, ["cpf_real", "cpf_real_titular"])
###

### funções de teste específicas para cada coluna
def test_column(df, column, test_values):
    if column in df.columns:
        valores_estranhos = [str(value) for value in df[column].unique() if str(value) not in test_values]
        if len(valores_estranhos) > 10:
            restantes = len(valores_estranhos) - 10
            warning(f'Coluna {column}, valores estranhos: {valores_estranhos[:10] + ["... mais " + str(restantes)]}')
        elif len(valores_estranhos) > 0:
            warning(f'Coluna {column}, valores estranhos: {valores_estranhos}')
        else:
            success(f'test_column: coluna "{column}" está ok')
def test_sexo(df):
    test_column(df, "sexo", ["masculino", "feminino"])


def test_status(df):
    test_column(df, "status", ["ativo", "não ativo", "inativo", "demitido.aposentado"])
    if "status" in df.columns:
        if "Inativo" in df["status"].unique():
            warning('test_status: é recomendável padronizar o valor "Inativo" para "Não ativo"')


def test_titularidade(df):
    test_column(df, "titularidade", [
        "titular", "dependente", 'companheiro(a)', 'filho(a)', 'pai.mãe', 'agregado(a)',
        'enteado(a)', 'outro', 'conjuge', 'irmão(ã)', 'sobrinho(a)', 'cunhado(a)', 'neto(a)',
        'trimo(a)', 'togro(a)', 'tio(a)', 'tutelado(a)'
    ])
    if "titularidade" in df.columns and "status" in df.columns:
        if "Outro(a)" in df["status"].unique():
            warning('test_status: é recomendável padronizar o valor "Outro(a)" para "Outro"')


def test_datas(df, test_columns, verify_nulls=True):
    for column in test_columns:
        if column in df.columns:
            if verify_nulls:
                tamanho = len(df)
                nulls = len(df[df[column].isna()])
                if nulls > 0:
                    warning(f'Coluna {column}: de {tamanho} rows, {nulls} estão null')
            valores_estranhos = []
            for valor in df[column].unique():
                if pd.isnull(valor):
                    continue
                try:
                    pd.to_datetime(valor)
                except:
                    valores_estranhos.append(str(valor))
            if len(valores_estranhos) > 10:
                warning(f'Coluna {column}, valores estranhos: {valores_estranhos[:10] + ["..."]}')
            elif len(valores_estranhos) > 0:
                warning(f'Coluna {column}, valores estranhos: {valores_estranhos}')


def test_float(df, test_columns):
    for column in test_columns:
        if column in df.columns:
            tamanho = len(df)
            nulls = len(df[df[column].isna()])
            if nulls > 0:
                warning(f'Coluna {column}: de {tamanho} rows, {nulls} estão null')
            valores_estranhos = []
            for valor in df[column].unique():
                if pd.isnull(valor):
                    continue
                try:
                    float(valor)
                except:
                    valores_estranhos.append(str(valor))
            if len(valores_estranhos) > 10:
                warning(f'Coluna {column}, valores estranhos: {valores_estranhos[:10] + ["..."]}')
            elif len(valores_estranhos) > 0:
                warning(f'Coluna {column}, valores estranhos: {valores_estranhos}')


def test_codigos(df, test_columns):
    for column in test_columns:
        if column in df.columns:
            tamanho = len(df)
            nulls = len(df[df[column].isna()])
            if nulls > 0:
                warning(f'Coluna {column}: de {tamanho} rows, {nulls} estão null')
            valores_em_float = []
            for valor in df[column].unique():
                if pd.isnull(valor):
                    continue
                elif ".0" in str(valor):
                    valores_em_float.append(str(valor))
            if len(valores_em_float) > 10:
                warning(f'Coluna {column}, valores estranhos: {valores_em_float[:10] + ["..."]}')
            elif len(valores_em_float) > 0:
                warning(f'Coluna {column}, valores estranhos: {valores_em_float}')

def test_nomes(df):
    for column in ["nome", "nome_beneficiario", "nome_titular"]:
        if column in df.columns:
            tamanho = len(df)
            nulls = len(df[df[column].isna()])
            if nulls > 0:
                warning(f'Coluna {column}: de {tamanho} rows, {nulls} estão null')
            nomes = {
                "numericos": [],
                "com espaco no inicio ou fim": [],
                "com espaco duplo": [],
                "com caracteres estranhos": [],
                "com siglas": []
            }
            for nome in df[column].unique():
                if pd.isnull(nome):
                    continue
                if len(re.findall('[0-9]', str(nome))) > 0:
                    nomes["numericos"].append(nome)
                if not str(nome).replace(' ', '').isalnum():
                    nomes["com caracteres estranhos"].append(nome)
                if len(re.findall('^[ ].*$', nome)) + len(re.findall('^.*[ ]$', nome)) > 0:
                    nomes["com espaco no inicio ou fim"].append(nome)
                if len(re.findall('[ ]{2}', str(nome))) > 0:
                    nomes["com espaco duplo"].append(nome)
                if len(re.findall(' [A-Za-z] ', nome)) > 0:
                    nomes["com siglas"].append(nome)
            success_flag = True
            for k, v in nomes.items():
                if len(v) > 0:
                    if k == "com siglas" and "sulamerica" in df["operadora"].unique()[0]:
                        warning(f'test_nomes: "{column}" -> {len(v)} nomes {k}, comum para operadora sulamerica')
                    else:
                        warning(f'test_nomes: "{column}" -> nomes {k}: {v}')
                    success_flag = False
            if success_flag:
                success(f'test_nomes: coluna "{column}" está ok')


def test_specifics(df):
    if "gndi" in str(df.operadora.unique()[0]).lower():
        # eventos com nome PARTICULAR e NÃO INFORMADO
        if "nome_beneficiario" in df.columns:
            num_particulares = len(df[df["nome_beneficiario"] == "PARTICULAR"])
            if num_particulares > 0:
                success(f"test_specifics: operadora gndi -> {num_particulares} eventos de PARTICULAR, removendo-os dos testes")
                df = df[df["nome_beneficiario"] != "PARTICULAR"]
            num_nao_informados = len(df[df["nome_beneficiario"] == "NÃO INFORMADO"])
            if num_nao_informados > 0:
                success(f"test_specifics: operadora gndi -> {num_nao_informados} eventos de NÃO INFORMADO, removendo-os dos testes")
                df = df[df["nome_beneficiario"] != "NÃO INFORMADO"]
    return df


def test_categoria_original(df):
    if "categoria_original" not in df.columns and "subcategoria" in df.columns:
        warning('"categoria_original" não está no df, se a "subcategoria" for categorização da operadora, adicione "save_columns" no final do pipeline')
    elif "categoria_original" not in df.columns:
        warning('"categoria_original" não está no df, se tiver a categorização da operadora no arquivo, favor salvá-la nesta coluna')


# EM DESENVOLVIMENTO
def test_nao_ativos(df):
    for empresa_id in df["empresa_id"].unique():
        df_empresa = df[df["empresa_id"] == empresa_id]

        # verifica pessoas com status != "Ativo" que estão com cancelamento_plano null
        if "status" in df_empresa.columns and "cancelamento_plano" in df_empresa.columns:
            nao_ativos_cancelamento_null = len(df_empresa[(df_empresa["status"] != "ativo") &
                                                    (df_empresa["cancelamento_plano"].isna())])
            if nao_ativos_cancelamento_null > 0:
                print(f'test_nao_ativos: empresa {empresa_id} tem {nao_ativos_cancelamento_null} registros com status != "ativo" e cancelamento_plano null')
        # TODO: verificar se a empresa está MTM e se o não ativo está com o cancelamento no fim do mês (deveria estar cancelamento = inclusao)


                    

