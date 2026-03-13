SELECT
    -- Estoques (Tabela Principal)
    EST."fk clientes",
    EST."fk vendedores",
    EST."fk empresas",
    EST."fk depositos" AS est_fk_depositos,
    EST."fk produtos" AS est_fk_produtos,
    EST."sistema origem" AS est_sistema_origem,
    EST.operacao,
    EST."codigo do produto",
    EST."codigo da empresa",
    EST."data ultima saida",
    EST."data ultima entrada",
    EST."data primeira entrada",
    EST."ultimo custo",
    EST.quantidade,
    EST."dias desde ultima saida",
    EST."dias desde ultima entrada",
    EST."idade do estoque",
    EST."dias desde primeira entrada",

    -- Produtos_final
    P."pk produtos",
    P."fk fabricante",
    P."sistema origem" AS prod_sistema_origem,
    P.codigo AS prod_codigo,
    P.nome AS prod_nome,
    P.grupo,
    P.subgrupo,
    P.colecao,
    P.fabricante,
    P.marca,
    P.linha,
    P."ultimo custo" AS prod_ultimo_custo,
    P."data primeira venda",
    P."dias desde primeira venda",
    P."codigo e nome",
    P.classe,
    P.codigo_comprador,
    P.nome_comprador,
    P.codigo_barras,

    -- Depositos_final
    D."fk depositos" AS dep_fk_depositos,
    D.deposito AS dep_nome

FROM belshop_platinum_db.Estoques AS EST
LEFT JOIN belshop_platinum_db.Produtos_final AS P 
    ON EST."fk produtos" = P."pk produtos"
LEFT JOIN belshop_platinum_db.Depositos_final AS D 
    ON EST."fk depositos" = D."fk depositos"