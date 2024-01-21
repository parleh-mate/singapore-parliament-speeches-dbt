{{
    config(
        materialized='view'
    )
}}

with type_cast as (
    select
        cast(date as date) as date,
        cast(member_name as string) as member_name,
        cast(is_present as bool) as is_present
    from {{ source('raw', 'attendance') }}
    where member_name is not null
)

select *
from type_cast
