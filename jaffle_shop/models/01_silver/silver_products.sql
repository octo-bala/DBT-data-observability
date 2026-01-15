with

products as (

    select * from {{ ref('bronze_products') }}

)

select * from products
