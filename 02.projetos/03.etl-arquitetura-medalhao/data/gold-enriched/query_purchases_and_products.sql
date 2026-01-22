SELECT
    p.purchase_id,
    u.id           AS user_id,
    u.nome         AS nome_usuario,
    u.email,
    u.telefone,
    u.cep,
    u.data_nascimento,

    c.localidade,
    c.estado,
    c.uf,
    c.bairro,
    c.regiao,

    pr.id          AS product_id,
    pr.nome        AS nome_produto,
    pr.categoria,
    pr.marca,
    pr.preco,

    p.quantidade,
    (p.quantidade * pr.preco) AS valor_total
FROM purchases p
INNER JOIN users u
    ON p.user_id = u.id
INNER JOIN products pr
    ON p.product_id = pr.id
INNER JOIN cep_info c
    ON u.cep = c.cep
ORDER BY p.purchase_id;
