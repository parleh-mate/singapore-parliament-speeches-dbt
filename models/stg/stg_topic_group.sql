{{
    config(
        materialized='view'
    )
}}

with type_cast as (
    select
        cast(topic_id as string) as topic_id,
        cast(highest_topic as int64) as topic_group_id,
        cast(highest_topic_distribution as float64) as topic_group_distribution
    from {{ source('topic_modelling', 'highest_topic_19_nmf_20240331') }}
)

select *
from type_cast
