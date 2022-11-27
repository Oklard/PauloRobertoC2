import datetime
from bson import ObjectId
import pandas as pd
from model.Veiculo import Veiculo
from model.Cliente import Cliente
from controller.controller_Cliente import Controller_Cliente
from conexion.mongo_queries import MongoQueries
from reports.relatorios import Relatorio

class Controller_Veiculo:
    def __init__(self):
        self.ctrl_cliente = Controller_Cliente()
        self.ctrl_Veiculo = Controller_Veiculo()
        self.mongo = MongoQueries()
        self.relatorio = Relatorio()
        
    def inserir_veiculo(self) -> Veiculo:
        # Cria uma nova conexão com o banco 
        self.mongo.connect()
        
        self.relatorio.get_relatorio_clientes()
        cpf = str(input("Digite o número do CPF do Cliente: "))
        cliente = self.valida_cliente(cpf)
        if cliente == None:
            return None

        # Lista os fornecedores existentes para inserir no pedido
        #self.relatorio.get_relatorio_fornecedores()
        #cnpj = str(input("Digite o número do CNPJ do Fornecedor: "))
        #fornecedor = self.valida_fornecedor(cnpj)
        #if fornecedor == None:
        #    return None

        data_hoje = datetime.today().strftime("%m-%d-%Y")
        proximo_veiculo = self.mongo.db["Veiculo"].aggregate([
                                                            {
                                                                '$group': {
                                                                    '_id': '$Veiculo', 
                                                                    'proximo_veiculo': {
                                                                        '$max': '$codCaro'
                                                                    }
                                                                }
                                                            }, {
                                                                '$project': {
                                                                    'proximo_carro': {
                                                                        '$sum': [
                                                                            '$proximo_veiculo', 1
                                                                        ]
                                                                    }, 
                                                                    '_id': 0
                                                                }
                                                            }
                                                        ])

        proximo_veiculo = int(list(proximo_veiculo)[0]['proximo_veiculo'])
        # Cria um dicionário para mapear as variáveis de entrada e saída
        data = dict(codCarro=proximo_veiculo, ano=data_hoje, cpf=cliente.get_CPF())
        # Insere e Recupera o código do novo pedido
        id_veiculo = self.mongo.db["Veiculo"].insert_one(data)
        # Recupera os dados do novo produto criado transformando em um DataFrame
        #revivsa se é recupera veiculo codigo ou so veiculo
        df_veiculo = self.recupera_veiculo(id_veiculo.inserted_id)
        # Cria um novo objeto Produto
        novo_veiculo = Veiculo(df_veiculo.codCarro.values[0], df_veiculo.ano.values[0], cliente)
        # Exibe os atributos do novo produto
        print(novo_veiculo.to_string())
        self.mongo.close()
        # Retorna o objeto novo_pedido para utilização posterior, caso necessário
        return novo_veiculo


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
            print(Veiculo_excluido.to_string())
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
    def valida_cliente(self, cpf:str=None) -> Cliente:
        if self.ctrl_cliente.verifica_existencia_cliente(cpf=cpf, external=True):
            print(f"O CPF {cpf} informado não existe na base.")
            return None
        else:
            # Recupera os dados do novo cliente criado transformando em um DataFrame
            df_cliente = self.ctrl_cliente.recupera_cliente(cpf=cpf, external=True)
            # Cria um novo objeto cliente
            cliente = Cliente(df_cliente.cpf.values[0], df_cliente.nome.values[0])
            return cliente
    def excluir_veiculo(self):
        # Cria uma nova conexão com o banco que permite alteração
        self.mongo.connect()

        # Solicita ao usuário o código do produto a ser alterado
        codigo_veiculo = int(input("Código do Pedido que irá excluir: "))        

        # Verifica se o produto existe na base de dados
        if not self.verifica_existencia_veiculo(codigo_veiculo):            
            # Recupera os dados do novo produto criado transformando em um DataFrame
            df_veiculo = self.recupera_veiculo_codigo(codigo_veiculo)
            cliente = self.valida_cliente(df_veiculo.cpf.values[0])
            
            opcao_excluir = input(f"Tem certeza que deseja excluir o pedido {codigo_veiculo} [S ou N]: ")
            if opcao_excluir.lower() == "s":
                print("Atenção, caso o pedido possua itens, também serão excluídos!")
                opcao_excluir = input(f"Tem certeza que deseja excluir o pedido {codigo_veiculo} [S ou N]: ")
                if opcao_excluir.lower() == "s":
                    # Revome o produto da tabela
                    self.mongo.db["Veiculo"].delete_one({"codigo_veiculo": codigo_veiculo})
                    print("Itens do pedido removidos com sucesso!")
                    self.mongo.db["Veiculo"].delete_one({"codigo_veiculo": codigo_veiculo})
                    # Cria um novo objeto Produto para informar que foi removido
                    veiculo_excluido = Veiculo(df_veiculo.codigo_veiculo.values[0], df_veiculo.ano.values[0], cliente)
                    self.mongo.close()
                    # Exibe os atributos do produto excluído
                    print("Pedido Removido com Sucesso!")
                    print(veiculo_excluido.to_string())
        else:
            self.mongo.close()
            print(f"O código {codigo_veiculo} não existe.")