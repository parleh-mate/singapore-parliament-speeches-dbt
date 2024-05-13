{{ config(materialized="table") }}

-- sources
with
    topics as (
        select topic_id, title, date, section_type
        from {{ ref("dim_topics") }}
        where section_type in ('BI', 'BP')
    ),

    -- 1. first readings
    first_reading as (
        select topic_id, '1 First' as reading from topics where section_type = 'BI'
    ),

    -- 2. second and third readings
    second_third_reading_topics as (
        select topic_id, title from topics where section_type = 'BP'
    ),

    flag_second_third_reading_from_speeches as (
        select
            speeches.topic_id,
            lower(speeches.text)
            like any ('%second reading%', '%second time%') as is_second_reading,
            lower(speeches.text)
            like any ('%third reading%', '%third time%') as is_third_reading
        from {{ ref("fact_speeches") }} as speeches
        inner join
            second_third_reading_topics
            on speeches.topic_id = second_third_reading_topics.topic_id
    ),

    summarise_flags_by_topic as (
        select
            topic_id,
            max(is_second_reading) as is_second_reading,
            max(is_third_reading) as is_third_reading
        from flag_second_third_reading_from_speeches
        group by topic_id
    ),

    second_reading as (
        select topic_id, '2 Second' as reading
        from summarise_flags_by_topic
        where is_second_reading
    ),

    third_reading as (
        select topic_id, '3 Third' as reading
        from summarise_flags_by_topic
        where is_third_reading
    ),

    unioned as (
        select topic_id, reading
        from first_reading
        union all
        select topic_id, reading
        from second_reading
        union all
        select topic_id, reading
        from third_reading
    ),

    joined as (
        select topics.date, unioned.topic_id, topics.title, unioned.reading
        from unioned
        left join topics on unioned.topic_id = topics.topic_id
    ),

    clean_title as (
        select
            * except (title),
            case
                when title = 'Central Provident Fund (Amendment No.2) Bill'
                then 'Central Provident Fund (Amendment No 2) Bill'
                when title = 'Co-operatives Societies (Amendment) Bill'
                then 'Co-operative Societies (Amendment) Bill'
                when title = 'Criminal Law (Temporary Provisions) Amendment Bill'
                then 'Criminal Law (Temporary Provisions) (Amendment) Bill'
                when title = 'Final Supply (FY2015) Bill'
                then 'Final Supply (FY 2015) Bill'
                when
                    title
                    = 'Economic Expansion Incentives (Relief From Income Tax) (Amendment) Bill'
                then
                    'Economic Expansion Incentives (Relief from Income Tax) (Amendment) Bill'
                when trim(title) = 'Financial Services and Markets (Amendments) Bill'
                then 'Financial Services and Markets (Amendment) Bill'
                when title = 'Good and Services Tax (Amendment) Bill'
                then 'Goods and Services Tax (Amendment) Bill'
                when title = 'Housing and Development Board (Amendment) Bill'
                then 'Housing and Development (Amendment) Bill'
                when title = 'Income Tax (Amendment) (No 3) Bill'
                then 'Income Tax (Amendment No 3) Bill'
                when
                    title
                    = 'Motor Vehicles (Third-party Risks and Compensation) (Amendment) Bill'
                then
                    'Motor Vehicles (Third-Party Risks and Compensation) (Amendment) Bill'
                when title = 'Second Supplementary Supply (2021) Bill'
                then 'Second Supplementary Supply (FY 2021) Bill'
                when title = 'Statue Law Reform Bill'
                then 'Statute Law Reform Bill'
                when title = 'Supplementary Supply (FY2016) Bill'
                then 'Supplementary Supply (FY 2016) Bill'
                when title = 'Supplementary Supply (FY2019) Bill'
                then 'Supplementary Supply (FY 2019) Bill'
                when trim(title) = 'Supply BIll'
                then 'Supply Bill'
                when title = 'Supplementary Supply (FY2017) Bill'
                then 'Supplementary Supply (FY 2017) Bill'
                when title = 'Supplementary Supply (FY2022) Bill'
                then 'Supplementary Supply (FY 2022) Bill'
                when title like 'Supplementary Supply (FY2023) Bill%'
                then 'Supplementary Supply (FY 2023) Bill'
                when
                    title
                    = 'Tobacco (Control of Advertisements and Sale (Amendment) Bill'
                then 'Tobacco (Control of Advertisements and Sale) (Amendment) Bill'
                else trim(title)
            end as title
        from joined
    )

select *
from clean_title
