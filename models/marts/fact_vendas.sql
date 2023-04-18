with
    clientes as (
        select *
        from {{ ref('dim_clientes') }}
    )
    , funcionarios as (
        select * 
        from {{ ref('dim_funcionarios') }}
    )
    , produtos as (
        select *
        from {{ ref('dim_produtos') }}
    )
    , pedido_itens as (
        select *
        from {{ ref('int_vendas__pedido_itens') }}
    )
    , joined_tabelas as (
        select
            pedido_itens.id_pedido
            , clientes.sk_cliente as fk_cliente
            , funcionarios.sk_funcionario as fk_funcionario
            , produtos.sk_produto as fk_produto
            , pedido_itens.id_transportadora
            , pedido_itens.desconto_perc
            , pedido_itens.preco_da_unidade
            , pedido_itens.quantidade
            , pedido_itens.data_do_pedido
            , pedido_itens.frete
            , pedido_itens.destinatario
            , pedido_itens.endereco_destinatario
            , pedido_itens.cep_destinatario
            , pedido_itens.cidade_destinatario
            , pedido_itens.regiao_destinatario
            , pedido_itens.pais_destinatario
            , pedido_itens.data_do_envio
            , pedido_itens.data_requerida_entrega
            , clientes.nome_cliente
            , funcionarios.funcionario_nome
            , funcionarios.gerente_nome
            , produtos.nome_produto
            , produtos.nome_categoria
            , produtos.nome_fornecedor
            , produtos.is_descontinuado
        from pedido_itens
        left join clientes on
            pedido_itens.id_cliente = clientes.id_cliente
        left join funcionarios on
            pedido_itens.id_funcionario = funcionarios.id_funcionario
        left join produtos on
            pedido_itens.id_produto = produtos.id_produto
    )
    , transformacoes as (
        select
            {{ dbt_utils.generate_surrogate_key(['id_pedido', 'fk_produto']) }} as sk_venda
            , *
            , preco_da_unidade * quantidade as total_bruto
            , (1 - desconto_perc) * (preco_da_unidade * quantidade) as total_liquido
            , case
                when desconto_perc > 0 then true
                when desconto_perc = 0 then false
                else false
            end as is_desconto
        from joined_tabelas
    )

    select *
    from transformacoes