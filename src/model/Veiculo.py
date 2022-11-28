from datetime import date


class Veiculo:
    def __init__(self, 
                 CodCarro:int=None, 
                 Modelo:str=None                  
                 ):
        self.set_CodCarro(CodCarro)
        self.set_Modelo(Modelo)
        
        ### SETTERS ###

    def set_CodCarro(self, CodCarro:int):
        self.CodCarro = CodCarro

    def set_Modelo(self, Modelo:str):
        self.Modelo = Modelo
            
       ### GETTERS ###

    def get_CodCarro(self) -> int:
        return self.CodCarro

    def get_Modelo(self) -> str:
        return self.Modelo


    def to_string(self) -> str:
        return f"Modelo: {self.get_Modelo()}"
