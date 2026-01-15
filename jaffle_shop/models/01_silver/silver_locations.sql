with

locations as (

    select * from {{ ref('bronze_locations') }}

)

select * from locations
