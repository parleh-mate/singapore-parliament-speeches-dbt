{{
    config(
        materialized='view'
    )
}}

with type_cast as (
    select
        cast(topic_id as string) as topic_id,
        cast(topic_order as int64) as topic_order,
        cast(title as string) as title,
        cast(section_type as string) as section_type,
        date(date) as date
    from {{ source('raw', 'topics') }}
)

select *
from type_cast
