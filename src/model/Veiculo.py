from datetime import date


class Veiculo:
    def __init__(self, 
                 CodCarro:int=None, 
                 Modelo:str=None, 
                 cor:str=None,
                 ano:date=None,
                 chassi:str=None,
                 tipoCambio:bool=None,
                 fabricante:str=None
                 ):
        self.set_CodCarro(CodCarro)
        self.set_Modelo(Modelo)
        self.set_cor(cor)
        self.set_ano(ano)
        self.set_chassi(chassi)
        self.set_tipoCambio(tipoCambio)
        self.set_fabricante(fabricante)
        
        ### SETTERS ###

    def set_CodCarro(self, CodCarro:int):
        self.CodCarro = CodCarro

    def set_Modelo(self, Modelo:str):
        self.Modelo = Modelo

    def set_cor(self, cor:str):
        self.cor = cor
            
    def set_ano(self, ano:date):
        self.ano = ano
         
    def set_chassi(self, chassi:str):
        self.chassi = chassi

    def set_tipoCambio(self, tipoCambio:bool):
        self.tipoCambio = tipoCambio
   
    def set_fabricante(self, fabricante:str):
        self.fabricante = fabricante

       ### GETTERS ###

    def get_CodCarro(self) -> int:
        return self.CodCarro

    def get_Modelo(self) -> str:
        return self.Modelo

    def get_cor(self) -> str:
        return self.cor
    
    def get_ano(self) -> date:
        return self.ano

    def get_chassi(self) -> str:
        return self.chassi
    
    def get_tipoCambio(self) -> bool:
        return self.tipoCambio
    
    def get_fabricante(self) -> str:
        return self.fabricante

    def to_string(self) -> str:
        return f"Modelo: {self.get_Modelo()} | Cor Principal: {self.get_cor()} Fabricante: {self.get_fabricante}"
    ### ADICIONAR O TO STRING DE CAMBIO ONDE 0 é MANUAL E 1 AUTOMATICO TOBEDONE ###