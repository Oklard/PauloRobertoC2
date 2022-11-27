from conexion.mongo_queries import MongoQueries
import pandas as pd

class Relatorio:
    
    def get_relatorio_Cliente(self):
        
        mongo = MongoQueries()
        mongo.connect()
        query_results = mongo.db['Cliente'].find({})
        df_cliente = pd.DataFrame(list(query_results))
        return df_cliente

    def get_relatorio_Veiculo(self):

        mongo = MongoQueries()
        mongo.connect()

        query_results = mongo.db['Veiculo'].find({})
        df_laboratorio = pd.DataFrame(list(query_results))
        return df_laboratorio

    def get_relatorio_clientes_Veic(self):
        
        mongo = MongoQueries()
        mongo.connect()
        pipeline = [
            { '$lookup': {
            'from': 'Cliente',
            'localField': 'idCliente',
            'foreignField': 'idCliente',
            'as': 'users'
            }},
            {"$unwind": { "path": "$users"}},
            {
            '$lookup': {
            'from': 'Veiculo',
            'localField': 'CodCarro',
            'foreignField': 'idCliente',
            'as': 'Veiculos'
            }
            },
            { "$unwind": {"path": "$Veiculo"}},
            { "$project": {
            "Nome_Cliente": "$users.nome",
            "data":1,
            "Tipo_Laboratorio": "$laboratorios.lab_tipo",
            "_id":0
            }}
        ]
        query_results = mongo.db['Veiculo'].aggregate(pipeline)
        df_relatorio = pd.DataFrame(list(query_results))
        print(df_relatorio)
        input("Pressione Enter para sair do relatório")

    def get_relatorio_total_clientes(self):

        mongo = MongoQueries()
        mongo.connect()

        pipeline = [
            {
            '$group': {
                '_id': '$idCliente',
                'codCarro': {
                    '$sum':1
                }
            }
            },
            {
            '$project': {
                'idCliente': '$_id',
                'codCarro':1,
                '_id':0
            }
            },
            {
            '$lookup': {
                'from': 'Veiculo',
                'localField': 'idCliente',
                'foreignField': 'idCliente',
                'as': 'agenda'
            }
            },
            {
            '$unwind': {
                'path':'$Veiculo'
            }
            },
            {
            '$project': {
            'idCliente':1,
            'codCarro':1,
            '_id':0
            }
            }
        ]
        query_results = mongo.db['Cliente'].aggregate(pipeline)
        df_relatorio = pd.DataFrame(list(query_results))
        print(df_relatorio)
        input("Pressione Enter para sair do relatório")