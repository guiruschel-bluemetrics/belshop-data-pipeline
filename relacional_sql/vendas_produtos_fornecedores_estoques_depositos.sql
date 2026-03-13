SELECT
    -- Vendas (Tabela Principal)
    V."codigo da empresa",
    V."codigo da venda",
    V."serie da venda",
    V."codigo do cliente",
    V."codigo do vendedor",
    V."data da venda",
    V."valor da venda bruta total",
    V."valor em vale-troca total",
    V."valor da venda liquida total",
    V."quantidade venda bruta total",
    V."quantidade troca total",
    V."quantidade venda liquida total",
    V."custo da venda total",
    V."custo de troca total",
    V.frete,
    V."codigo do produto" AS vda_codigo_produto,
    V."quantidade venda itens",
    V."valor da venda itens",
    V."custo da venda itens",
    V."quantidade troca itens",
    V."valor de troca itens",
    V."custo de troca itens",
    V.hora,
    V."quantidade venda liquida itens",
    V."valor da venda liquida itens",
    V."custo da venda liquida itens",
    V."uk venda",
    V."faixa de valor",
    V."mes de referencia",
    V."fk cliente",
    V."fk vendedores",
    V."fk empresas",
    V."fk produtos",
    V."sistema origem" AS vda_sistema_origem,
    V.operacao,
    V."desconto total",

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

    -- Fornecedores_final
    F."pk fabricante" AS forn_pk_fabricante,
    F."sistema origem" AS forn_sistema_origem,
    F.cidade AS forn_cidade,
    F.codigo AS forn_codigo,
    F.data_cadastro AS forn_data_cadastro,
    F.nome AS forn_nome,
    F.status AS forn_status,
    F.uf AS forn_uf,
    F.etl_updated_at AS forn_etl_updated_at,

    -- Empresas
    E."pk empresas",
    E."codigo da filial",
    E."codigo da loja",
    E.nome AS emp_nome,
    E."nome reduzido" AS emp_nome_reduzido,
    E.cidade AS emp_cidade,
    E.uf AS emp_uf,
    E.pais AS emp_pais,
    E."rede da loja",
    E.latitude,
    E.longitude,
    E.agrupamento,
    E."supervisor loja",
    E."supervisor salao",
    E."area da loja",

    -- Estoques (Adicionado)
    EST.quantidade AS est_quantidade,
    EST."ultimo custo" AS est_ultimo_custo,
    EST."idade do estoque" AS est_idade_estoque,
    EST."data ultima entrada" AS est_data_ultima_entrada,
    EST."data ultima saida" AS est_data_ultima_saida,
    EST."fk depositos" AS est_fk_depositos,

-- data_das_transacoes (Dimensão Tempo)
    D.DATA AS calend_data,
    D."data ano anterior" AS calend_data_ano_anterior,
    D.ytd AS calend_ytd,
    D.ytt AS calend_ytt,
    D."dia da semana" AS calend_dia_semana,
-- Depositos_final
    D."fk depositos" AS dep_fk_depositos,
    D.deposito AS dep_nome

FROM belshop_platinum_db.Vendas AS V
LEFT JOIN belshop_platinum_db.Produtos_final AS P 
    ON V."fk produtos" = P."pk produtos"
LEFT JOIN belshop_platinum_db.Fornecedores_final AS F 
    ON P."fk fabricante" = F."pk fabricante"
LEFT JOIN belshop_platinum_db.Empresas AS E 
    ON V."fk empresas" = E."pk empresas"
LEFT JOIN belshop_platinum_db.Estoques AS EST
    ON V."fk produtos" = EST."fk produtos" 
    AND V."fk empresas" = EST."fk empresas"
LEFT JOIN belshop_platinum_db.data_das_transacoes AS D 
    ON V."data da venda" = D.DATA
LEFT JOIN belshop_platinum_db.Depositos_final AS D 
    ON EST."fk depositos" = D."fk depositos"