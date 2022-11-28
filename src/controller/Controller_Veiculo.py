from bson import ObjectId
import pandas as pd
from model.Veiculo import Veiculo
from conexion.mongo_queries import MongoQueries

class Controller_Veiculo:
    def __init__(self):
        self.mongo = MongoQueries()
        
    def inserir_veiculo(self) -> Veiculo:
        # Cria uma nova conexão com o banco
        self.mongo.connect()
        
        #Solicita ao usuario a nova descrição do produto
        descricao_novo_veiculo = input("Descrição (Novo): ")
        proximo_veiculo = self.mongo.db["Veiculo"].aggregate([
                                                    {
                                                        '$group': {
                                                            '_id': '$Veiculos', 
                                                            'proximo_veiculo': {
                                                                '$max': '$codigo_veiculo'
                                                            }
                                                        }
                                                    }, {
                                                        '$project': {
                                                            'proximo_veiculo': {
                                                                '$sum': [
                                                                    '$proximo_produto', 1
                                                                ]
                                                            }, 
                                                            '_id': 0
                                                        }
                                                    }
                                                ])

        proximo_veiculo = str(list(proximo_veiculo)[0]['proximo_veiculo'])
        
        # Insere e Recupera o código do novo produto
        id_veiculo = self.mongo.db["Veiculo"].insert_one({"codigo_veiculo": proximo_veiculo, "descricao_veiculo": descricao_novo_veiculo})
        # Recupera os dados do novo produto criado transformando em um DataFrame
        df_veiculo = self.recupera_veiculo(id_veiculo.inserted_id)
        # Cria um novo objeto Produto
        novo_veiculo = Veiculo(df_veiculo.codigo_veiculo.values[0], df_veiculo.descricao_veiculo.values[0])
        # Exibe os atributos do novo produto
        print(novo_veiculo.to_string())
        self.mongo.close()
        # Retorna o objeto novo_produto para utilização posterior, caso necessário
        return novo_veiculo

    def atualizar_veiculo(self) -> Veiculo:
        # Cria uma nova conexão com o banco que permite alteração
        self.mongo.connect()

        # Solicita ao usuário o código do produto a ser alterado
        codigo_veiculo = int(input("Código do Produto que irá alterar: "))        

        # Verifica se o produto existe na base de dados
        if not self.verifica_existencia_veiculo(codigo_veiculo):
            # Solicita a nova descrição do produto
            nova_descricao_veiculo = input("Descrição (Novo): ")
            # Atualiza a descrição do produto existente
            self.mongo.db["Veiculo"].update_one({"codigo_veiculo": codigo_veiculo}, {"$set": {"descricao_produto": nova_descricao_veiculo}})
            # Recupera os dados do novo produto criado transformando em um DataFrame
            df_veiculo = self.recupera_veiculo_codigo(codigo_veiculo)
            # Cria um novo objeto Produto
            veiculo_atualizado = Veiculo(df_veiculo.codigo_veiculo.values[0], df_veiculo.descricao_veiculo.values[0])
            # Exibe os atributos do novo produto
            print(veiculo_atualizado.to_string())
            self.mongo.close()
            # Retorna o objeto produto_atualizado para utilização posterior, caso necessário
            return veiculo_atualizado
        else:
            self.mongo.close()
            print(f"O código {codigo_veiculo} não existe.")
            return None

    def excluir_veiculo(self):
        # Cria uma nova conexão com o banco que permite alteração
        self.mongo.connect()

        # Solicita ao usuário o código do produto a ser alterado
        codigo_veiculo = int(input("Código do Produto que irá excluir: "))        

        # Verifica se o produto existe na base de dados
        if not self.verifica_existencia_veiculo(codigo_veiculo):            
            # Recupera os dados do novo produto criado transformando em um DataFrame
            df_veiculo = self.recupera_veiculo_codigo(codigo_veiculo)
            # Revome o produto da tabela
            self.mongo.db["Veiculo"].delete_one({"codigo_veiculo": codigo_veiculo})
            # Cria um novo objeto Produto para informar que foi removido
            veiculo_excluido = Veiculo(df_veiculo.codigo_veiculo.values[0], df_veiculo.descricao_veiculo.values[0])
            # Exibe os atributos do produto excluído
            print("Produto Removido com Sucesso!")
            print(veiculo_excluido.to_string())
            self.mongo.close()
        else:
            self.mongo.close()
            print(f"O código {codigo_veiculo} não existe.")

    def verifica_existencia_veiculo(self, codigo:int=None, external: bool = False) -> bool:
        if external:
            # Cria uma nova conexão com o banco que permite alteração
            self.mongo.connect()

        # Recupera os dados do novo produto criado transformando em um DataFrame
        df_veiculo = pd.DataFrame(self.mongo.db["Veiculo"].find({"codigo_veiculo":codigo}, {"codigo_veiculo": 1, "descricao_veiculo": 1, "_id": 0}))

        if external:
            # Fecha a conexão com o Mongo
            self.mongo.close()

        return df_veiculo.empty

    def recupera_veiculo(self, _id:ObjectId=None) -> pd.DataFrame:
        # Recupera os dados do novo produto criado transformando em um DataFrame
        df_veiculo = pd.DataFrame(list(self.mongo.db["Veiculo"].find({"_id":_id}, {"codigo_veiculo": 1, "descricao_veiculo": 1, "_id": 0})))
        return df_veiculo

    def recupera_veiculo_codigo(self, codigo:int=None, external: bool = False) -> pd.DataFrame:
        if external:
            # Cria uma nova conexão com o banco que permite alteração
            self.mongo.connect()

        # Recupera os dados do novo produto criado transformando em um DataFrame
        df_veiculo = pd.DataFrame(list(self.mongo.db["Veiculo"].find({"codigo_veiculo":codigo}, {"codigo_veiculo": 1, "descricao_veiculo": 1, "_id": 0})))

        if external:
            # Fecha a conexão com o Mongo
            self.mongo.close()

        return df_veiculo