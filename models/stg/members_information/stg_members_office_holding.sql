with
    source as (select * from {{ source("raw", "members_office_holding") }}),
    renamed as (
        select
            {{ adapter.quote("member_name") }} as member_name,
            {{ adapter.quote("position") }} as member_appointment,
            cast({{ adapter.quote("from_date") }} as date) as effective_from_date,
            cast({{ adapter.quote("to_date") }} as date) as effective_to_date
        from source
    ),

    -- union manually-filled information
    manual_gsheet as (
        select member_name, member_appointment, effective_from_date, effective_to_date
        from {{ ref("stg_gsheet_office_holding") }}
    ),

    unioned as (
        select member_name, member_appointment, effective_from_date, effective_to_date
        from renamed
        union all
        select member_name, member_appointment, effective_from_date, effective_to_date
        from manual_gsheet
    ),

    add_latest_flag as (
        select
            *,
            effective_to_date
            is null  -- when it is null, this is the current appointment
            as is_latest_appointment
        from unioned
    )
select *
from add_latest_flag
