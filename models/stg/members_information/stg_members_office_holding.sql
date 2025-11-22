with
    source as (select * from {{ source("raw", "members_office_holding") }}),

    current_members as (
        select distinct member_name
        from source
        where accessed_at >= (select max(accessed_at) from source)
    ),
    clean_raw as (
        select
            member_name,
            position,
            from_date,
            case
                when
                    to_date is null
                    and member_name not in (select member_name from current_members)
                then '2025-04-14'
                else to_date
            end as to_date,
            case
                when
                    to_date is null
                    and member_name not in (select member_name from current_members)
                then replace(date_range, 'Current', '14 April 2025')
                else date_range
            end as date_range,
            accessed_at
        from source
        -- extract non current members, and for current members, take only most
        -- recently scraped info
        where
            member_name not in (select member_name from current_members)
            or (
                member_name in (select member_name from current_members)
                and accessed_at = (select max(accessed_at) from source)
            )
    ),

    renamed as (
        select
            member_name,
            {{ adapter.quote("position") }} as member_appointment,
            cast({{ adapter.quote("from_date") }} as date) as effective_from_date,
            cast({{ adapter.quote("to_date") }} as date) as effective_to_date,
            cast(accessed_at as date) as accessed_at
        from clean_raw
    ),

    -- union manually-filled information
    manual_gsheet as (
        select *
        from {{ ref("stg_gsheet_office_holding") }}
    ),

    unioned as (
        select member_name, member_appointment, effective_from_date, effective_to_date, accessed_at
        from renamed
        union all
        select member_name, member_appointment, effective_from_date, effective_to_date, accessed_at
        from manual_gsheet
    ),

     -- filter latest non null entries --
    filter_latest as (
        select member_name, member_appointment, effective_from_date, effective_to_date
        from unioned
        where member_appointment is not null
        qualify row_number() over(
            partition by member_name, 
            member_appointment, 
            effective_from_date, 
            effective_to_date 
            order by accessed_at desc
        ) = 1
    ),

    add_latest_flag as (
        select
            *,
            effective_to_date
            is null  -- when it is null, this is the current appointment
            as is_latest_appointment
        from filter_latest
    )
select *
from add_latest_flag
