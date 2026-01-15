with

supplies as (

    select * from {{ ref('bronze_supplies') }}

)

select * from supplies
