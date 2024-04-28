with
    unioned as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("stg_members_member_of_parliament"),
                    ref("stg_members_office_holding"),
                    ref("stg_members_select_committee"),
                ],
                source_column_name="type",
            )
        }}
    ),

    renamed as (
        select
            member_name,
            case
                when
                    type
                    = '`singapore-parliament-speeches`.`prod_stg`.`stg_members_member_of_parliament`'
                then 'constituency'
                when
                    type
                    = '`singapore-parliament-speeches`.`prod_stg`.`stg_members_office_holding`'
                then 'appointment'
                when
                    type
                    = '`singapore-parliament-speeches`.`prod_stg`.`stg_members_select_committee`'
                then 'select_committee'
            end as type,
            coalesce(
                member_constituency, member_appointment, member_committee
            ) as member_position,
            effective_from_date,
            effective_to_date,
            coalesce(
                is_latest_constituency, is_latest_appointment, is_latest_committee
            ) as is_latest_position
        from unioned
    )

select *
from renamed
-- temporary filter so it's not confusing
-- information from earlier parliaments are not included
where not (effective_from_date is null and effective_to_date is null)
