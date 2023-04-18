with
    funcionarios as (
        select
            *
        from {{ ref('stg_erp__funcionarios') }}
    )

    , self_join_gerentes as (
        select
            funcionarios.id_funcionario
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
            funcionarios.id_gerente = gerentes.id_funcionario
    )

    , transformacoes as (
        select
            row_number() over(order by id_funcionario) as sk_funcionario
            , *
        from self_join_gerentes
    )

select *
from transformacoes