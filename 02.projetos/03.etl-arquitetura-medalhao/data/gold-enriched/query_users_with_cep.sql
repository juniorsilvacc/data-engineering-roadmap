SELECT 
    DISTINCT
    u.id,
    u.nome,
    u.email,
    u.telefone,
    u.cep,
    u.data_nascimento,
    c.localidade,
    c.estado,
    c.uf,
    c.ddd,
    c.bairro,
    c.regiao
FROM users u
INNER JOIN cep_info c ON u.cep = c.cep
ORDER BY u.id;
