with
    fonte_funcionarios as (
        select *
        from {{ source('erp', 'employees') }}
    )
    , renomear as (
        select 
            cast(employee_id as int) as funcionario_id
            , cast(last_name as string) as funcionario_sobrenome
            , cast(first_name as string) as funcionario_nome
            , cast(birth_date as date) as funcionario_data_nascimento
            , cast(hire_date as date) as funcionario_data_contratacao
            , cast(address as string) as funcionario_endereco
            , cast(city as string) as funcionario_cidade
            , cast(region as string) as funcionario_regiao
            , cast(postal_code as string) as funcionario_cep
            , cast(country as string) as funcionario_pais
            , cast(notes as string) as funcionario_notas
            , cast(reports_to as int) as id_gerente
        from fonte_funcionarios
    )
select *
from renomear