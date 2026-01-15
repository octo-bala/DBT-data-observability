-- metricflow_time_spine.sql
with

days as (

     select 
        dateadd(day, number, '2000-01-01') as date_day
    from (
        select top (365 * 10) 
            row_number() over (order by (select null)) - 1 as number
        from sys.all_objects a
        cross join sys.all_objects b
    ) as numbers

),

cast_to_date as (

    select cast(date_day as date) as date_day

    from days

)

select * from cast_to_date
