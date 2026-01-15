with

source as (

    select  
        id as order_item_id,
        order_id,
        sku as product_id 
        from {{ source('ecom', 'raw_items') }}

),

renamed as (

    select

        ----------  ids
        order_item_id,
        order_id,
        product_id 

    from source

)

select * from renamed
