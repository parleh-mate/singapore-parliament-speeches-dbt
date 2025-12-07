with
    source as (select * from {{ source("raw", "members_office_holding") }}),

    parliament_dates as (select * from {{ ref("stg_gsheet_parliament_dates") }}),

    renamed as (
        select
            member_name,
            {{ adapter.quote("position") }} as member_appointment,
            cast({{ adapter.quote("from_date") }} as date) as effective_from_date,
            cast({{ adapter.quote("to_date") }} as date) as effective_to_date,
            cast(accessed_at as date) as accessed_at
        from source
    ),

    current_members as (
        select distinct member_name
        from renamed
        where accessed_at = (select max(accessed_at) from renamed)
    ),
    clean_raw as (
        select
            renamed.member_name,
            renamed.member_appointment,
            renamed.effective_from_date,
            case
                when
                    renamed.effective_to_date is null
                    and renamed.member_name not in (select member_name from current_members)
                then pd.to_date
                else renamed.effective_to_date
            end as effective_to_date,
            renamed.accessed_at
        from renamed
        left join parliament_dates pd
        on renamed.effective_from_date between pd.from_date and pd.to_date
        -- extract non current members, and for current members, take only most
        -- recently scraped info
        where
            renamed.member_name not in (select member_name from current_members)
            or (
                renamed.member_name in (select member_name from current_members)
                and renamed.accessed_at = (select max(accessed_at) from renamed)
            )
    ),

    -- union manually-filled information
    manual_gsheet as (
        select *
        from {{ ref("stg_gsheet_office_holding") }}
    ),

    unioned as (
        select member_name, member_appointment, effective_from_date, effective_to_date, accessed_at
        from clean_raw
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
            effective_from_date
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
