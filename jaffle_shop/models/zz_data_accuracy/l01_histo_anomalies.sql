{{ config(materialized='view', tags=["quality"]) }}

with 
    histo_anomalies as (select 
                          * 
                        from  {{ source('sources_common_audit','HISTO_FAILURES_TABLE')}})
    ,monitor as (select *
                        ,ROW_NUMBER() OVER (PARTITION BY nam_test ORDER BY dat_execution asc) as rank_monitor
                 from {{ source('sources_common_audit','MONITOR_TABLE')}})
    ,join_table as (select histo_anomalies.*
                    ,monitor.dat_execution
                    ,monitor.nam_test
                    ,monitor.rank_monitor
                    ,ROW_NUMBER() OVER (PARTITION BY id_context, nam_test ORDER BY dat_execution asc) as rank_histo
                    from histo_anomalies
                    left join monitor on histo_anomalies.id_run = monitor.id_run
    )

select
    id_anomaly,
    id_run,
    id_context,
    anomaly_column_value,
    dat_execution,
    nam_test,
    rank_monitor,
    rank_histo,
    rank_monitor - rank_histo as id_session,
    min(rank_monitor) OVER (
        PARTITION BY
            id_context,
            nam_test,
            rank_monitor - rank_histo
    ) as min_rn_monitor,
    max(rank_monitor) OVER (
        PARTITION BY
            id_context,
            nam_test,
            rank_monitor - rank_histo
    ) + 1 as max_rn_monitor
from join_table
