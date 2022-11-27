select c.idcliente
     , c.nome 
     , c.cpf
     , c.email
     , c.telefone
     , c.endereco
  from labdatabase.Cliente c
 order by c.nome