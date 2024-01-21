{{
    config(
        materialized='table'
    )
}}

-- sources

with attendance as (
    select
        date,
        member_name,
        is_present
    from {{ ref('stg_attendance') }}
),

seed_member as (
    select
        mp_name as member_name,
        party,
        gender
    from {{ ref('member') }}
),

-- transform

agg_attendance as (
    select
        member_name,
        min(date) as earliest_sitting,
        max(date) as latest_sitting,
        count(distinct if(is_present, date, null)) as count_sittings_present,
        count(distinct date) as count_sittings_total
    from attendance
    group by member_name
),

-- join

joined as (
    select
        agg.member_name,
        seed.party,
        seed.gender,
        agg.earliest_sitting,
        agg.latest_sitting,
        agg.count_sittings_present,
        agg.count_sittings_total
    from agg_attendance as agg
    left join seed_member as seed
        on agg.member_name = seed.member_name
)

select *
from joined
