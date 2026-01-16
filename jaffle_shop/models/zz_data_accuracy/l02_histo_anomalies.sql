{{ config(materialized='view', tags=["quality"]) }}

with 
    histo_anomalies as (select * from {{ ref('l01_histo_anomalies')}})
    ,monitor as (select *
                        ,ROW_NUMBER() OVER (PARTITION BY nam_test ORDER BY dat_execution asc) as rank_monitor
                 from {{ source('sources_common_audit','MONITOR_TABLE')}})

select
    h.id_anomaly,
    h.id_run,
    h.id_context,
    h.anomaly_column_value,
    h.dat_execution,
    h.nam_test,
    h.rank_monitor,
    h.rank_histo,
    h.id_session,
    h.min_rn_monitor,
    h.max_rn_monitor,
    case
        when m2.rank_monitor = h.min_rn_monitor then 1
        else 0
    end as flg_apparition,
    m1.dat_execution as dat_first_apparition_without_error,
    m2.dat_execution as dat_first_apparition,
    case
        when m2.rank_monitor = h.min_rn_monitor then h.id_anomaly
        else null
    end as id_anomaly_first_apparition
from
    histo_anomalies h
    left join monitor m1 on h.nam_test = m1.nam_test
    and h.max_rn_monitor = m1.rank_monitor
    left join monitor m2 on h.nam_test = m2.nam_test
    and h.min_rn_monitor = m2.rank_monitor
