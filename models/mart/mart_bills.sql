{{ config(materialized="table") }}


with
    bill_activity as (
        select
            year,
            title,
            first_reading_date,
            first_reading_topic,
            second_reading_date,
            second_reading_topic,
            third_reading_date,
            third_reading_topic
        from {{ ref("mart_prep_bill_activity") }}
    )

select
    year,
    title,
    first_reading_date,
    first_reading_topic,
    second_reading_date,
    second_reading_topic,
    third_reading_date,
    third_reading_topic,
    date_diff(
        second_reading_date, first_reading_date, day
    ) as day_diff_first_second_reading,
    date_diff(
        third_reading_date, second_reading_date, day
    ) as day_diff_second_third_reading
from bill_activity
