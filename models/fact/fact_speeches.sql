{{
    config(
        materialized='table'
    )
}}

with source as (
    select
        speech_id,
        date,
        topic_id,
        speech_order,
        member_name,
        text
    from {{ ref('stg_speeches') }}
)

select *
from source
