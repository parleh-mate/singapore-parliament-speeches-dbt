{{
    config(
        materialized='table'
    )
}}

with source as (
    select
        topic_id,
        date,
        topic_order,
        title,
        section_type
    from {{ ref('stg_topics') }}
)

select *
from source
