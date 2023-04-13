with
    funcionarios as (
        select
            funcionario_id
            , id_gerente
            , funcionario_nome
            , funcionario_data_nascimento
            , funcionario_data_contratacao
            , funcionario_endereco
            , funcionario_cidade
            , funcionario_regiao
            , funcionario_cep
            , funcionario_pais
            , funcionario_notas
        from {{ ref('stg_erp__funcionarios') }}
    )

    , self_join_gerentes as (
        select
            funcionarios.funcionario_id
            , funcionarios.id_gerente
            , funcionarios.funcionario_nome
            , gerentes.funcionario_nome as gerente_nome
            , funcionarios.funcionario_data_nascimento
            , funcionarios.funcionario_data_contratacao
            , funcionarios.funcionario_endereco
            , funcionarios.funcionario_cidade
            , funcionarios.funcionario_regiao
            , funcionarios.funcionario_cep
            , funcionarios.funcionario_pais
            , funcionarios.funcionario_notas
        from funcionarios
        left join funcionarios as gerentes on
            funcionarios.id_gerente = gerentes.funcionario_id
    )

    , transformacoes as (
        select
            row_number() over(order by funcionario_id) as sk_funcionarios
            , *
        from self_join_gerentes
    )

select *
from transformacoes