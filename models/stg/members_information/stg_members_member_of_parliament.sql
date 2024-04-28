with
    source as (select * from {{ source("raw", "members_member_of_parliament") }}),

    renamed as (
        select
            {{ adapter.quote("member_name") }} as member_name,
            {{ adapter.quote("constituency") }} as member_constituency,
            {{ adapter.quote("from_date") }} as effective_from_date,
            {{ adapter.quote("to_date") }} as effective_to_date,
            {{ adapter.quote("date_range") }} as date_range  -- for conditional only

        from source
    ),

    process as (
        -- if members had served across two or more terms, they should be combined
        select
            member_name,
            member_constituency,
            min(effective_from_date) as effective_from_date,
            case
                when countif(lower(date_range) like '%current%') = 1
                then null
                else max(effective_to_date)
            end as effective_to_date
        from renamed
        group by all
    ),

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
        from process
    )
select *
from add_latest_flag
