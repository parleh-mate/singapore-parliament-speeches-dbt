with
    source as (select * from {{ source("raw", "members_member_of_parliament") }}),

    current_members as (
        select distinct member_name
        from source
        where accessed_at >= (select max(accessed_at) from source)
    ),
    clean_raw as (
        select
            member_name,
            constituency,
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
            {{ adapter.quote("member_name") }} as member_name,
            {{ adapter.quote("constituency") }} as member_constituency,
            cast({{ adapter.quote("from_date") }} as date) as effective_from_date,
            cast({{ adapter.quote("to_date") }} as date) as effective_to_date,
            {{ adapter.quote("date_range") }} as date_range,  -- for conditional only
            cast(accessed_at as date) as accessed_at

        from source
    ),

    process as (
        -- if members had served across two or more terms, they should be combined
        select
            member_name,
            trim(replace(member_constituency, chr(160), ' ')) as member_constituency,
            effective_from_date,
            case
                when lower(date_range) like '%current%' then null else effective_to_date
            end as effective_to_date,
            accessed_at
        from renamed
    ),

    -- union manually-filled information
    manual_gsheet as (
        select
            member_name,
            trim(replace(member_constituency, chr(160), ' ')) as member_constituency,
            effective_from_date,
            effective_to_date,
            accessed_at
        from {{ ref("stg_gsheet_member_of_parliament") }}
    ),

    unioned as (
        select member_name, member_constituency, effective_from_date, effective_to_date, accessed_at
        from process
        union all
        select member_name, member_constituency, effective_from_date, effective_to_date, accessed_at
        from manual_gsheet
    ),

     -- filter latest non null entries --
    filter_latest as (
        select member_name, member_constituency, effective_from_date, effective_to_date
        from unioned
        where member_constituency is not null
        qualify row_number() over(
            partition by 
            member_name, 
            member_constituency, 
            effective_from_date, 
            effective_to_date 
            order by accessed_at desc
        ) = 1
    ),

    -- post-process
    add_latest_flag as (
        select
            *,
            case
                when
                    countif(effective_to_date is null) over (partition by member_name)
                    = 1
                then effective_to_date is null  -- when it is null, this is the current appointment
                when
                    countif(effective_to_date is null) over (partition by member_name)
                    = 0
                then
                    effective_to_date
                    = max(effective_to_date) over (partition by member_name)
            end as is_latest_constituency
        from filter_latest
    )
select *
from add_latest_flag
