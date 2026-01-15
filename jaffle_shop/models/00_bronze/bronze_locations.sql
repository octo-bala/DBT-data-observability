with

source as (

    select * from {{ source('ecom', 'raw_stores') }}

),

locations as (

    select
        id as location_id,
        name as location_name,
        tax_rate,
        {{ dbt.date_trunc('day', 'opened_at') }} as opened_date

    from source

)

select * from locations
