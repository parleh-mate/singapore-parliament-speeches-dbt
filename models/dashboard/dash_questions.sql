with
    by_parl_questions as (
        select
            member_name,
            cast(parliament as string) as parliament,
            member_constituency,
            member_party,
            ministry_addressed,
            count(*) as count_questions_ministry
        from {{ ref("mart_speeches") }}
        where
            is_primary_question
            and member_party is not null
            and ministry_addressed is not null
        group by
            member_name,
            parliament,
            member_constituency,
            member_party,
            ministry_addressed
    ),
    all_parl_questions as (
        select
            member_name,
            'All' as parliament,
            'All' as member_constituency,
            member_party,
            ministry_addressed,
            count(*) as count_questions_ministry
        from {{ ref("mart_speeches") }}
        where
            is_primary_question
            and member_party is not null
            and ministry_addressed is not null
        group by member_name, member_party, ministry_addressed
    )
select *
from by_parl_questions
union all
select *
from all_parl_questions
