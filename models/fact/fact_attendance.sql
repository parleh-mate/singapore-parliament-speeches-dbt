{{ config(materialized="table") }}

with source as (select date, member_name, is_present from {{ ref("stg_attendance") }})

select *
from source
