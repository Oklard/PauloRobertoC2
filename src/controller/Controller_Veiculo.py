from bson import ObjectId
import pandas as pd
from model.Veiculo import Veiculo
from conexion.mongo_queries import MongoQueries

class Controller_Veiculo:
    def __init__(self):
        self.mongo = MongoQueries()
        
    def inserir_veiculo(self) -> Veiculo:
        # Cria uma nova conexão com o banco que permite alteração
        self.mongo.connect()

        # Solicita ao usuario o novo CPF
        CodCarro = input("CodCarro (Novo): ")

        if self.verifica_existencia_veiculo(CodCarro):
            # Solicita ao usuario o novo nome
            descricao_veiculo = input("descricao veiculo (Novo): ")
            # Insere e persiste o novo cliente
            self.mongo.db["Veiculo"].insert_one({"CodCarro": CodCarro, "descricao_veiculo": descricao_veiculo})
            # Recupera os dados do novo cliente criado transformando em um DataFrame
            df_veiculo = self.recupera_veiculo(CodCarro)
            # Cria um novo objeto Cliente
            novo_veiculo = Veiculo(df_veiculo.CodCarro.values[0], df_veiculo.descricao_veiculo.values[0])
            # Exibe os atributos do novo cliente
            print(novo_veiculo.to_string())
            self.mongo.close()
            # Retorna o objeto novo_cliente para utilização posterior, caso necessário
            return novo_veiculo
        else:
            self.mongo.close()
            print(f"O Codigo do carro {CodCarro} já está cadastrado.")
            return None

    def atualizar_veiculo(self) -> Veiculo:
        # Cria uma nova conexão com o banco que permite alteração
        self.mongo.connect()

        # Solicita ao usuário o código do produto a ser alterado
        CodCarro = input("Código do Veiculo que irá alterar: ")

        # Verifica se o produto existe na base de dados
        if not self.verifica_existencia_veiculo(CodCarro):
            # Solicita a nova descrição do produto
            nova_descricao_veiculo = input("Descrição (Novo): ")
            # Atualiza a descrição do produto existente
            self.mongo.db["Veiculo"].update_one({"CodCarro": CodCarro}, {"$set": {"descricao_veiculo": nova_descricao_veiculo}})
            # Recupera os dados do novo produto criado transformando em um DataFrame
            df_veiculo = self.recupera_veiculo_codigo(CodCarro)
            # Cria um novo objeto Produto
            veiculo_atualizado = Veiculo(df_veiculo.CodCarro.values[0], df_veiculo.descricao_veiculo.values[0])
            # Exibe os atributos do novo produto
            print(veiculo_atualizado.to_string())
            self.mongo.close()
            # Retorna o objeto produto_atualizado para utilização posterior, caso necessário
            return veiculo_atualizado
        else:
            self.mongo.close()
            print(f"O código {CodCarro} não existe.")
            return None

    def excluir_veiculo(self):
        # Cria uma nova conexão com o banco que permite alteração
        self.mongo.connect()

        # Solicita ao usuário o código do produto a ser alterado
        CodCarro = input("Código do Veiculo que irá excluir: ")      

        # Verifica se o produto existe na base de dados
        if not self.verifica_existencia_veiculo(CodCarro):            
            # Recupera os dados do novo produto criado transformando em um DataFrame
            df_veiculo = self.recupera_veiculo_codigo(CodCarro)
            # Revome o produto da tabela
            self.mongo.db["Veiculo"].delete_one({"CodCarro": CodCarro})
            # Cria um novo objeto Produto para informar que foi removido
            veiculo_excluido = Veiculo(df_veiculo.CodCarro.values[0], df_veiculo.descricao_veiculo.values[0])
            # Exibe os atributos do produto excluído
            print("Veiculo Removido com Sucesso!")
            print(veiculo_excluido.to_string())
            self.mongo.close()
        else:
            self.mongo.close()
            print(f"O código {CodCarro} não existe.")

    def verifica_existencia_veiculo(self, codigo:str=None, external: bool = False) -> bool:
        if external:
            # Cria uma nova conexão com o banco que permite alteração
            self.mongo.connect()

        # Recupera os dados do novo produto criado transformando em um DataFrame
        df_veiculo = pd.DataFrame(self.mongo.db["Veiculo"].find({"CodCarro":codigo}, {"CodCarro": 1, "descricao_veiculo": 1, "_id": 0}))

        if external:
            # Fecha a conexão com o Mongo
            self.mongo.close()

        return df_veiculo.empty

    def recupera_veiculo(self, _id:ObjectId=None) -> pd.DataFrame:
        # Recupera os dados do novo produto criado transformando em um DataFrame
        df_veiculo = pd.DataFrame(list(self.mongo.db["Veiculo"].find({"_id":_id}, {"CodCarro": 1, "descricao_veiculo": 1, "_id": 0})))
        return df_veiculo

    def recupera_veiculo_codigo(self, codigo:str=None, external: bool = False) -> pd.DataFrame:
        if external:
            # Cria uma nova conexão com o banco que permite alteração
            self.mongo.connect()

        # Recupera os dados do novo produto criado transformando em um DataFrame
        df_veiculo = pd.DataFrame(list(self.mongo.db["Veiculo"].find({"CodCarro":codigo}, {"CodCarro": 1, "descricao_veiculo": 1, "_id": 0})))

        if external:
            # Fecha a conexão com o Mongo
            self.mongo.close()

        return df_veiculo
