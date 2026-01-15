with

source as (

    select  
        id as order_item_id,
        order_id,
        sku as product_id 
        from {{ source('ecom', 'raw_items') }}

),

order_item as (

    select
        order_item_id,
        order_id,
        product_id 

    from source

)

select * from order_item
