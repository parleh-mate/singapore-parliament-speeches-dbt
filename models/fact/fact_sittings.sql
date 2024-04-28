{{ config(materialized="table") }}

with
    source as (
        select date, datetime, parliament, session, volume, sittings
        from {{ ref("stg_sittings") }}
    )

select *
from source
