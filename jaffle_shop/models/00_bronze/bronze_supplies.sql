with

source as (

    select * from {{ source('ecom', 'raw_supplies') }}

),

supplies as (

   select
        convert(varchar(32), hashbytes('MD5', concat(
            coalesce(cast(id as varchar(max)), '_dbt_utils_surrogate_key_null_'),
            coalesce(cast(sku as varchar(max)), '_dbt_utils_surrogate_key_null_')
        )), 2) as supply_uuid,
        id as supply_id,
        sku as product_id,
        name as supply_name,
        {{ cents_to_dollars('cost') }} as supply_cost,
        perishable as is_perishable_supply

    from source

)

select * from supplies
