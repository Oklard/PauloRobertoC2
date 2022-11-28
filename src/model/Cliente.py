class Cliente:
    def __init__(self,
                 CPF:str=None,
                 nome:str=None
                ):
        self.set_CPF(CPF)
        self.set_nome(nome)
        
        ### SETTERS ###
    def set_idCliente(self, idCliente:int):
        self.idCliente = idCliente

    def set_nome(self, nome:str):
        self.nome = nome

    def set_CPF(self, CPF:str):
        self.CPF = CPF
    
    def set_email(self, email:str):
        self.email = email

    def set_telefone(self, telefone:str):
        self.telefone = telefone

    def set_endereco(self, endereco:str):
        self.endereco = endereco

        ### GETTERS ###
    def get_idCliente(self) -> int:
        return self.idCliente
    def get_nome(self) -> str:
        return self.nome
    def get_CPF(self) -> str:
        return self.CPF
    def get_email(self) -> str:
        return self.email
    def get_telefone(self) -> str:
        return self.telefone
    def get_endereco(self) -> str:
        return self.endereco

    def to_string(self) -> str:
        return f"CPF: {self.get_CPF()} | Nome: {self.get_nome()}"
