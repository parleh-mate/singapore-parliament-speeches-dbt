{{ config(materialized="table") }}


with
    bill_activity as (
        select reading, topic_id, date, title, extract(year from date) as year
        from {{ ref("fact_bill_activity") }}
    ),

    summarise as (
        select
            year,
            replace(trim(title), '.', '') as title,
            -- 1 first reading
            min(case when left(reading, 1) = '1' then date end) as first_reading_date,
            min_by(
                topic_id, case when left(reading, 1) = '1' then date end
            ) as first_reading_topic,
            -- 2 second reading
            min(case when left(reading, 1) = '2' then date end) as second_reading_date,
            min_by(
                topic_id, case when left(reading, 1) = '2' then date end
            ) as second_reading_topic,
            -- 3 third reading
            max(case when left(reading, 1) = '3' then date end) as third_reading_date,
            max_by(
                topic_id, case when left(reading, 1) = '3' then date end
            ) as third_reading_topic
        from bill_activity
        group by 1, 2
    ),

    -- process bills where dates were joined across two years
    complete as (
        select
            summarise.year,
            summarise.title,
            summarise.first_reading_date,
            summarise.first_reading_topic,
            summarise.second_reading_date,
            summarise.second_reading_topic,
            summarise.third_reading_date,
            summarise.third_reading_topic
        from summarise
        where not (first_reading_date is null or second_reading_date is null)
    ),

    incomplete as (
        select
            summarise.year,
            summarise.title,
            summarise.first_reading_date,
            summarise.first_reading_topic,
            summarise.second_reading_date,
            summarise.second_reading_topic,
            summarise.third_reading_date,
            summarise.third_reading_topic,
            summarise.year + 1 as year_plus_1
        from summarise
        where first_reading_date is null or second_reading_date is null
    ),

    -- it is possible that the bill passage was raised across 2 years, to find this
    -- information
    incomplete_combine_across_different_years as (
        select
            i0.year,
            i0.title,
            coalesce(
                i0.first_reading_date, i1.first_reading_date
            ) as first_reading_date,
            coalesce(
                i0.first_reading_topic, i1.first_reading_topic
            ) as first_reading_topic,
            coalesce(
                i0.second_reading_date, i1.second_reading_date
            ) as second_reading_date,
            coalesce(
                i0.second_reading_topic, i1.second_reading_topic
            ) as second_reading_topic,
            coalesce(
                i0.third_reading_date, i1.third_reading_date
            ) as third_reading_date,
            coalesce(
                i0.third_reading_topic, i1.third_reading_topic
            ) as third_reading_topic
        from incomplete as i0
        left join incomplete as i1 on i0.year_plus_1 = i1.year and i0.title = i1.title
        -- to drop rows where information has been filled
        qualify
            not lag(i0.first_reading_date, 1) over (
                partition by i0.title order by i0.year
            )
            is not null
    ),

    unioned as (
        select
            year,
            title,
            first_reading_date,
            first_reading_topic,
            second_reading_date,
            second_reading_topic,
            third_reading_date,
            third_reading_topic
        from incomplete_combine_across_different_years
        union all
        select
            year,
            title,
            first_reading_date,
            first_reading_topic,
            second_reading_date,
            second_reading_topic,
            third_reading_date,
            third_reading_topic
        from complete
    ),

    clean_amendments as (
        select
            * except (title), regexp_replace(title, r'(No)\s(\d)', r'\1. \2') as title
        from unioned
    )

select *
from clean_amendments
