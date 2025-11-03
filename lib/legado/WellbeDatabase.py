import yaml
import pandas as pd
from sqlalchemy import create_engine, text
import pathlib
from sshtunnel import SSHTunnelForwarder
import os
import urllib.parse


class WellbeDatabase:
    def __init__(self, db_conf):
        if not os.path.exists(f"{db_conf}"):
            raise Exception(f"{db_conf} não encontrado na pasta wellbe_scripts, certifique se que o arquivo e o nome estejam corretos")

        with open(f"{db_conf}", "r") as stream:
            try:
                db_conf = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)

        print(
            "WellbeDatabase: banco {} no endereço {}, dev {}".format(
                db_conf["schema"], db_conf["host"], db_conf["dev"]
            )
        )

        self.host = urllib.parse.quote_plus(db_conf["host"])
        self.username = urllib.parse.quote_plus(db_conf["username"])
        self.password = urllib.parse.quote_plus(db_conf["password"])
        self.schema = urllib.parse.quote_plus(db_conf["schema"])

        self.engine = None
        self.connection = None
        self.mycursor = None

        print("Verificando conexão: ", end='')
        self.connect()
        print("Ok")
        print("Instância WellbeDatabase criada com sucesso!")

    def connect(self):
        self.engine = create_engine(
            f'mysql+pymysql://{self.username}:{self.password}@{self.host}:3306/{self.schema}',
            connect_args={
                #'ssl_verify_cert': True,
                'autocommit': True,
                "ssl": {"ssl_disabled": True}
            }
        )
        self.connection = self.engine.raw_connection()
        self.mycursor = self.connection.cursor()

    def dispose(self):
        self.connection.close()
        self.engine = None
        self.connection = None
        self.mycursor = None

    def getEngine(self):
        self.bindConnection()
        return self.engine

    def bindConnection(self):
        try:
            self.mycursor.execute("SELECT 1 FROM BI_Empresa where id = 3")
        except:
            try:
                self.dispose()
            finally:
                print("WellbeDatabase: Erro de conexão, reconectando: ", end='')
            self.connect()
            print("Ok")


    def runQuery(self, query, verbose=True, show_count=True):
        self.bindConnection()
        if verbose:
            print("executando:")
            print(query)
            print("\n\n\n --------------------------------------")
        a = self.mycursor.execute(query)
        self.connection.commit()
        if show_count:
            print(a)
        return a

    def runQueryWithPandas(self, query):
        self.bindConnection()
        df = pd.io.sql.read_sql(text(query), self.getEngine())
        return df

    def generateStamp(
            self,
            empresa_id,
            arquivo,
            descricao,
            force=False,
            operadora="",
            script_version=0,
            script_name="",
    ):
        """
        Função responsável por criar o carimbo.

        Args:
            empresa_id: str
                Id da empresa.
            arquivo: str
                Nome do arquivo
            descricao: str
                Descrição do carimbo
            force: boolean
                Força criação do carimbo
            operadora: str
                Operadora da empresa
            script_version: int
                Versão do script utilizado
            script_name: str
                Nome do script utilizado
        Returns:
            int
                Id do carimbo
        """
        self.bindConnection()
        operadora = operadora if operadora is None else f"'{operadora}'"
        script_name = script_name if script_name is None else f"'{script_name}'"

        carimbo = self.getEngine().execute(
            """
            insert into BI_Carimbo (empresa_id, data, arquivo, descricao, operadora, script_version, script_name) values ({}, now(), '{}', '{}', {}, {}, {})
            """.format(
                empresa_id, arquivo, descricao, operadora, script_version, script_name
            ),
            verbose=False,
        ).lastrowid
        return carimbo

    def getOperadora(self, empresa_id):
        data = self.runQueryWithPandas(
            "select * from BI_Empresa where id = {}".format(empresa_id)
        )["operadora"][0]
        return data

    def getCorretora(self, empresa_id):
        data = self.runQueryWithPandas(
            "select * from BI_Empresa where id = {}".format(empresa_id)
        )["corretora"][0]
        return data