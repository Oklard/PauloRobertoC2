from bson import ObjectId
import pandas as pd
from model.Veiculo import Veiculo
from conexion.mongo_queries import MongoQueries

class Controller_Veiculo:
    def __init__(self):
        self.mongo = MongoQueries()
        
    def inserir_Veiculo(self) -> Veiculo:
        # Cria uma nova conexão com o banco
        self.mongo.connect()
        
        # Lista os pedido existentes para inserir no item de pedido
        self.relatorios.get_relatorio_Veiculo()
        codCarro = int(str(input("Digite o número do Veiculo: ")))
        Veiculo = self.valida_veiculo(codCarro)
        if Veiculo == None:
            return None

        proximo_item_pedido = self.mongo.db["Veiculo"].aggregate([
                                                    {
                                                        '$group': {
                                                            '_id': '$Veiculo', 
                                                            'proximo_veiculo': {
                                                                '$max': '$codCarro'
                                                            }
                                                        }
                                                    }, {
                                                        '$project': {
                                                            'proximo_veiculo': {
                                                                '$sum': [
                                                                    '$proximo_veiculo', 1
                                                                ]
                                                            }, 
                                                            '_id': 0
                                                        }
                                                    }
                                                ])

        proximo_veiculo = int(list(proximo_item_pedido)[0]['proximo_item_pedido'])
        # Cria um dicionário para mapear as variáveis de entrada e saída
        data = dict(codigo_item_pedido=proximo_item_pedido, valor_unitario=valor_unitario, quantidade=quantidade, codigo_pedido=int(pedido.get_codigo_pedido()), codigo_produto=int(produto.get_codigo()))
        # Insere e Recupera o código do novo item de pedido
        id_item_pedido = self.mongo.db["itens_pedido"].insert_one(data)
        # Recupera os dados do novo item de pedido criado transformando em um DataFrame
        df_item_pedido = self.recupera_item_pedido(id_item_pedido.inserted_id)
        # Cria um novo objeto Item de Pedido
        novo_item_pedido = ItemPedido(df_item_pedido.codigo_item_pedido.values[0], df_item_pedido.quantidade.values[0], df_item_pedido.valor_unitario.values[0], pedido, produto)
        # Exibe os atributos do novo Item de Pedido
        print(novo_item_pedido.to_string())
        self.mongo.close()
        # Retorna o objeto novo_item_pedido para utilização posterior, caso necessário
        return novo_item_pedido

    def atualizar_veiculo(self) -> Veiculo:
        # Cria uma nova conexão com o banco que permite alteração
        self.mongo.connect()

        # Solicita ao usuário o código do veiculo a ser alterado
        codigo_veiculo = int(input("Código do veiculo que irá alterar: "))        

        # Verifica se o veiculo existe na base de dados
        if not self.verifica_existencia_veiculo(codigo_veiculo):
            # Solicita a nova descrição do veiculo
            nova_descricao_veiculo = input("Descrição (Novo): ")
            # Atualiza a descrição do veiculo existente
            self.mongo.db["Veiculo"].update_one({"codigo_veiculo": codigo_veiculo}, {"$set": {"descricao_veiculo": nova_descricao_veiculo}})
            # Recupera os dados do novo veiculo criado transformando em um DataFrame
            df_veiculo = self.recupera_veiculo_codigo(codigo_veiculo)
            # Cria um novo objeto veiculo
            veiculo_atualizado = Veiculo(df_veiculo.codigo_veiculo.values[0], df_veiculo.descricao_veiculo.values[0])
            # Exibe os atributos do novo veiculo
            print(veiculo_atualizado.to_string())
            self.mongo.close()
            # Retorna o objeto veiculo_atualizado para utilização posterior, caso necessário
            return veiculo_atualizado
        else:
            self.mongo.close()
            print(f"O código {codigo_veiculo} não existe.")
            return None

    def excluir_veiculo(self):
        # Cria uma nova conexão com o banco que permite alteração
        self.mongo.connect()

        # Solicita ao usuário o código do veiculo a ser alterado
        codigo_veiculo = int(input("Código do Veiculo que irá excluir: "))        

        # Verifica se o veiculo existe na base de dados
        if not self.verifica_existencia_veiculo(codigo_veiculo):            
            # Recupera os dados do novo veiculo criado transformando em um DataFrame
            df_veiculo = self.recupera_veiculo_codigo(codigo_veiculo)
            # Revome o veiculo da tabela
            self.mongo.db["Veiculo"].delete_one({"codigo_veiculo": codigo_veiculo})
            # Cria um novo objeto veiculo para informar que foi removido
            Veiculo_excluido = Veiculo(df_veiculo.codigo_veiculo.values[0], df_veiculo.descricao_veiculo.values[0])
            # Exibe os atributos do veiculo excluído
            print("veiculo Removido com Sucesso!")
            print(veiculo_excluido.to_string())
            self.mongo.close()
        else:
            self.mongo.close()
            print(f"O código {codigo_veiculo} não existe.")

    def verifica_existencia_veiculo(self, codigo:int=None, external: bool = False) -> bool:
        if external:
            # Cria uma nova conexão com o banco que permite alteração
            self.mongo.connect()

        # Recupera os dados do novo veiculo criado transformando em um DataFrame
        df_veiculo = pd.DataFrame(self.mongo.db["Veiculo"].find({"codigo_veiculo":codigo}, {"codCarro": 1, "descricao_veiculo": 1, "_id": 0}))

        if external:
            # Fecha a conexão com o Mongo
            self.mongo.close()

        return df_veiculo.empty
    
    
    def recupera_item_pedido(self, _id:ObjectId=None) -> bool:
        # Recupera os dados do novo pedido criado transformando em um DataFrame
        df_veiculo = pd.DataFrame(list(self.mongo.db["Veiculo"].find({"_id": _id}, {"codCarro":1, "modelo": 1, "cor": 1, "chassi": 1, "ano": 1,"tipoCambio": 1,"fabricante": 1,  "_id": 0})))
        return df_veiculo

    def recupera_veiculo_codigo(self, codigo:int=None) -> bool:
        # Recupera os dados do novo pedido criado transformando em um DataFrame
        df_veiculo = pd.DataFrame(list(self.mongo.db["Veiculo"].find({"codCarro": codigo}, {"codCarro":1, 
                                                                                                          "idCliente": 1, 
                                                                                                          "modelo": 1, 
                                                                                                          "cor": 1, 
                                                                                                          "chassi": 1,
                                                                                                          "ano": 1,
                                                                                                          "tipoCambio": 1,
                                                                                                          "fabricante": 1, 
                                                                                                          "_id": 0})))
        return df_veiculo

    def recupera_veiculo_codigo(self, codigo:int=None, external: bool = False) -> pd.DataFrame:
        if external:
            # Cria uma nova conexão com o banco que permite alteração
            self.mongo.connect()

        # Recupera os dados do novo veiculo criado transformando em um DataFrame
        df_veiculo = pd.DataFrame(list(self.mongo.db["Veiculo"].find({"codigo_veiculo":codigo}, {"codigo_veiculo": 1, "descricao_veiculo": 1, "_id": 0})))

        if external:
            # Fecha a conexão com o Mongo
            self.mongo.close()

        return df_veiculo