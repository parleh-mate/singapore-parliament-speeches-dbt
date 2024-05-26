with
    primary_questions as (
        select
            parliament,
            extract(year from date) as year,
            extract(month from date) as month,
            member_name,
            member_constituency,
            ministry_addressed
        from {{ ref("mart_speeches") }}
        where
            is_primary_question = true
            and ministry_addressed is not null
            and member_name != ''
    ),

    aggregated as (
        select
            parliament,
            year,
            month,
            member_name,
            member_constituency,
            ministry_addressed,
            count(*) as count_pri_questions
        from primary_questions
        group by all
    )

select *
from aggregated
order by member_name, parliament, year, month
