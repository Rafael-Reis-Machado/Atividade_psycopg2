import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

class Banco:

    conexao = None
    cursor = None
    dbname = ''
    usuario = 'postgres'
    senha = '123'
    porta = '5432'

    def __init__(self, dbname):
        # o bloco except será excecutado somente se ocorrer alguma exceção no bloco try
        try:
            # cria uma conexão com o banco postgres para poder criar o dbname
            self.conexao = psycopg2.connect(database="postgres", user=self.usuario, password=self.senha,port=self.porta)
            self.conexao.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)  # para evitar trabalhar com start transaction, commit e rollback
            self.cursor = self.conexao.cursor()  # obtém o cursor de acesso ao BD
            self.dbname = dbname
            if self.existBD() == False: # cria o BD se ele não existir
                self.cursor.execute("create database %s" % self.dbname)
                # verifica se o BD foi de fato criado
                if self.existBD() == True:
                    print("Database %s criado com sucesso" % self.dbname)
                else:
                    print("Problemas para criar o %s" % self.dbname)
            self.closeConexao()
        except Exception as e:
            print(e)

    def openConexao(self):
        # cria uma conexão com o banco dbname
        self.conexao = psycopg2.connect(database=self.dbname, user=self.usuario, password=self.senha, port=self.porta)
        self.conexao.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)  # para evitar trabalhar com start transaction, commit e rollback
        self.cursor = self.conexao.cursor()  # obtém o cursor de acesso ao BD

    def closeConexao(self):
        self.conexao.close() # encerra a conexão com o banco postgres

    def existBD(self):
        # verifica se o BD existe
        self.cursor.execute("select count(*) from pg_catalog.pg_database WHERE datname = %s", [self.dbname])
        if self.cursor.fetchone()[0] == 1:
            return True
        else:
            return False

    def existTable(self):
        # verifica se o BD existe
        self.cursor.execute("select count(*) from information_schema.tables where table_name='tbcliente'")
        if self.cursor.fetchone()[0] == 1:
            return True
        else:
            return False

    def dropDatabase(self):
        try:
            # cria uma conexão com o banco postgres para poder criar o dbname
            self.conexao = psycopg2.connect(database="postgres", user=self.usuario, password=self.senha,port=self.porta)
            self.conexao.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)  # para evitar trabalhar com start transaction, commit e rollback
            self.cursor = self.conexao.cursor()  # obtém o cursor de acesso ao BD
            if self.existBD() == True:  # remove o BD se ele existir
                self.cursor.execute("drop database %s" % self.dbname)
                # verifica se o BD foi de fato removido
                if self.existBD() == False:
                    print("Database %s removido com sucesso" % self.dbname)
                else:
                    print("Problemas para remover o %s" % self.dbname)
                self.dbname = ''
            self.closeConexao()
        except Exception as e:
            print(e)

    def createTable(self):
        try:
            self.openConexao()
            clausula = "CREATE TABLE if not exists tbcliente(" + \
                            "idcliente serial PRIMARY KEY," + \
                            "nome varchar(50)," + \
                        "idade integer)"
            self.cursor.execute(clausula)
            # verifica se a tabela foi criada
            if self.existTable() == True:
                print("tbcliente criada com sucesso")
            else:
                print("tbcliente não foi criada")
            self.closeConexao()
        except Exception as e:
            print(e)

    def dropTable(self):
        try:
            self.openConexao()
            clausula = "drop table if exists tbcliente"
            self.cursor.execute(clausula)
            if self.existTable() == False:
                print("tbcliente removida com sucesso")
            else:
                print("tbcliente não foi removida")
            self.closeConexao()
        except Exception as e:
            print(e)

    def insert(self, nome, idade):
        try:
            self.openConexao()
            if self.existTable() == True:
                self.cursor.execute("insert into tbcliente(nome,idade) values(%s,%s)", [nome, idade])
            else:
                print("tbcliente não existe")
            self.closeConexao()
        except Exception as e:
            print(e)

    def select(self):
        try:
            self.openConexao()
            if self.existTable() == True:
                self.cursor.execute("select * from tbcliente order by idcliente")
                linhas = self.cursor.fetchall()
                for linha in linhas:
                    print(linha)
            else:
                print("tbcliente não existe")
            self.closeConexao()
        except Exception as e:
            print(e)

    def delete(self, idcliente):
        try:
            self.openConexao()
            if self.existTable() == True:
                self.cursor.execute("delete from tbcliente where idcliente=%s", [idcliente])
            else:
                print("tbcliente não existe")
            self.closeConexao()
        except Exception as e:
            print(e)

    def update(self, idcliente, nome, idade):
        try:
            self.openConexao()
            if self.existTable() == True:
                self.cursor.execute("update tbcliente set nome=%s, idade=%s where idcliente=%s",
                    [nome, idade, idcliente])
            else:
                print("tbcliente não existe")
            self.closeConexao()
        except Exception as e:
            print(e)

