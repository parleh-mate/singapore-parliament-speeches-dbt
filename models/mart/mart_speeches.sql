{{
    config(
        materialized='incremental',
        unique_key='speech_id',
        partition_by={
            "field": "date",
            "data_type": "date",
            "granularity": "day"
        },
        on_schema_change: "sync_all_columns",
        incremental_strategy: "insert_overwrite"
    )
}}

-- sources

with speeches as (
    select 
        speech_id,
        date,
        topic_id,
        member_name,
        text
    from {{ ref('fact_speeches') }}
    {% if is_incremental() %}
    where date >= (select max(date) from {{ this }})
    {% endif %}
),

topics as (
    select
        topic_id,
        title,
        section_type
    from {{ ref('dim_topics')}}
    {% if is_incremental() %}
    where date >= (select max(date) from {{ this }})
    {% endif %}
),

sittings as (
    select
        date,
        parliament,
        session,
        volume,
        sittings
    from {{ ref('fact_sittings')}}
    {% if is_incremental() %}
    where date >= (select max(date) from {{ this }})
    {% endif %}
),

members as (
    select
        member_name,
        party,
        gender
    from {{ ref('dim_members')}}
),

joined as (
    select
        -- metadata
        speeches.date,
        speeches.speech_id,
        speeches.topic_id,
        sittings.parliament,
        sittings.session,
        sittings.volume,
        sittings.sittings,

        -- member information
        speeches.member_name,
        members.party as member_party,
        members.gender as member_gender,

        -- topic information
        topics.title as topic_title,
        topics.section_type as topic_type,

        -- speech information
        speeches.text as speech_text

    from speeches
    left join topics
        on speeches.topic_id = topics.topic_id
    left join sittings
        on speeches.date = sittings.date
    left join members
        on speeches.member_name = members.member_name
)

select *
from joined
