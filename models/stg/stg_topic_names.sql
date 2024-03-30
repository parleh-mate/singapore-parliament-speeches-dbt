{{
    config(
        materialized='view'
    )
}}

with type_cast as (
    select
        cast(topic as int64) as topic_group_id,
        cast(topic_summary as string) as topic_group_summary,
        cast(top_n_words as string) as topic_group_top_n_words
    from {{ source('topic_modelling', 'topic_names_16_nmf_20240330') }}
)

select *
from type_cast
