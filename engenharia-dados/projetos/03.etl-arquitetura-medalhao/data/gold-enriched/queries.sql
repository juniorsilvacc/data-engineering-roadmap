# Total gasto por usuário
SELECT
    u.id,
    u.nome,
    SUM(p.quantidade * pr.preco) AS total_gasto
FROM purchases p
JOIN users u ON p.user_id = u.id
JOIN products pr ON p.product_id = pr.id
GROUP BY u.id, u.nome
ORDER BY total_gasto DESC;

# Produtos mais vendidos
SELECT
    pr.nome,
    SUM(p.quantidade) AS total_vendido
FROM purchases p
JOIN products pr ON p.product_id = pr.id
GROUP BY pr.nome
ORDER BY total_vendido DESC;

# Vendas por estado (análise geográfica)
SELECT
    c.uf,
    SUM(p.quantidade * pr.preco) AS faturamento
FROM purchases p
JOIN users u ON p.user_id = u.id
JOIN cep_info c ON u.cep = c.cep
JOIN products pr ON p.product_id = pr.id
GROUP BY c.uf
ORDER BY faturamento DESC;
