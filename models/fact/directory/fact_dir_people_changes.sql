with
    -- standardise names
    records_names as (
        select
            name as original_name,
            email,
            position,
            department_url,
            ministry_name,
            -- first run had some values in 2024-05-06, should be grouped as part of
            -- 2024-05-07
            if(
                cast(_accessed_at as date) = '2024-05-06',
                '2024-05-07',
                cast(_accessed_at as date)
            ) as _accessed_at
        from {{ ref("stg_dir_records_names") }}
        where
            not (
                name in ('', '-', '--')
                or name is null
                or email in ('-')
                or email is null
            )
    ),

    names_mapping as (
        select extracted_name as name, name as original_name
        from {{ ref("stg_dir_preprocess_names_mapping") }}
    ),

    standardise_names as (
        select
            names_mapping.name,
            records_names.email,
            records_names.position,
            records_names.department_url,
            records_names.ministry_name,
            records_names._accessed_at
        from records_names
        left join
            names_mapping on records_names.original_name = names_mapping.original_name
    ),

    -- this table, for now, only applies to people with one position
    get_positions_across_dates as (
        select
            _accessed_at,
            email,
            count(
                distinct concat(position, department_url, ministry_name)
            ) as count_positions
        from standardise_names
        group by all
    ),

    people_only_one_position as (
        select email
        from get_positions_across_dates
        group by all
        having max(count_positions) = 1
    ),

    -- limit to those with only one position now
    filter_standardise_names as (
        select *
        from standardise_names
        where email in (select email from people_only_one_position)
    ),

    -- analyses
    add_lag_values as (
        -- looking for new joiners and position changes
        select
            _accessed_at,
            name,
            email,
            position,
            department_url,
            ministry_name,
            lag(_accessed_at) over (
                partition by name, email order by _accessed_at
            ) as lag_accessed_at,
            lag(position) over (
                partition by name, email order by _accessed_at
            ) as lag_position,
            lag(department_url) over (
                partition by name, email order by _accessed_at
            ) as lag_department_url,
            lag(ministry_name) over (
                partition by name, email order by _accessed_at
            ) as lag_ministry_name,
        from filter_standardise_names
    ),

    lag_filter_out_similar as (
        select *
        from add_lag_values
        -- this is the first value, so lag values don't mean anything
        where
            _accessed_at > '2024-05-07'
            and (
                position != lag_position
                or department_url != lag_department_url
                or ministry_name != lag_ministry_name
            )
    ),

    new_joiners as (
        select
            'new joiner' as activity,
            _accessed_at as activity_date,
            name,
            email,
            position,
            department_url,
            ministry_name,
            cast(null as string) as old_information,
            lag_accessed_at as compared_against_date
        from lag_filter_out_similar
        where
            lag_position is null
            and lag_department_url is null
            and lag_ministry_name is null
    ),

    ministry_change as (
        select
            'change ministry' as activity,
            _accessed_at as activity_date,
            name,
            email,
            position,
            department_url,
            ministry_name,
            concat(
                "(",
                lag_ministry_name,
                ", ",
                lag_position,
                ", ",
                lag_department_url,
                ")"
            ) as old_information,
            lag_accessed_at as compared_against_date
        from lag_filter_out_similar
        where ministry_name != lag_ministry_name
    ),

    department_change_different_role as (
        select
            'change department (different role)' as activity,
            _accessed_at as activity_date,
            name,
            email,
            position,
            department_url,
            ministry_name,
            concat("(", lag_position, ", ", lag_department_url, ")") as old_information,
            lag_accessed_at as compared_against_date
        from lag_filter_out_similar
        where
            ministry_name = lag_ministry_name
            and department_url != lag_department_url
            and position != lag_position
    ),

    department_change_same_role as (
        select
            'change department (same role)' as activity,
            _accessed_at as activity_date,
            name,
            email,
            position,
            department_url,
            ministry_name,
            lag_department_url as old_information,
            lag_accessed_at as compared_against_date
        from lag_filter_out_similar
        where
            ministry_name = lag_ministry_name
            and department_url != lag_department_url
            and position = lag_position
    ),

    same_department_different_role as (
        select
            'change role (same department)' as activity,
            _accessed_at as activity_date,
            name,
            email,
            position,
            department_url,
            ministry_name,
            lag_position as old_information,
            lag_accessed_at as compared_against_date
        from lag_filter_out_similar
        where
            ministry_name = lag_ministry_name
            and department_url = lag_department_url
            and position != lag_position
    ),

    max_date as (select max(_accessed_at) - 1 as latest_run from records_names),

    add_lead_values as (
        -- looking for resignations
        select
            _accessed_at,
            name,
            email,
            position,
            department_url,
            ministry_name,
            lead(_accessed_at) over (
                partition by name, email order by _accessed_at
            ) as lead_accessed_at,
            lead(position) over (
                partition by name, email order by _accessed_at
            ) as lead_position,
            lead(department_url) over (
                partition by name, email order by _accessed_at
            ) as lead_department_url,
            lead(ministry_name) over (
                partition by name, email order by _accessed_at
            ) as lead_ministry_name
        from filter_standardise_names
    ),

    lead_filter_out_similar as (
        select *
        from add_lead_values
        -- this is the last value, so lead values don't mean anything
        where
            _accessed_at < (select latest_run from max_date)
            and (
                lead_position is null
                or lead_department_url is null
                or lead_ministry_name is null
            )
    ),

    resignees as (
        select
            'resigned' as activity,
            _accessed_at as activity_date,
            name,
            email,
            position,
            department_url,
            ministry_name,
            cast(null as string) as old_information,
            lead_accessed_at as compared_against_date
        from lead_filter_out_similar
    ),

    -- union activities
    unioned as (
        select *
        from new_joiners
        union all
        select *
        from ministry_change
        union all
        select *
        from department_change_different_role
        union all
        select *
        from department_change_same_role
        union all
        select *
        from same_department_different_role
        union all
        select *
        from resignees
    )

select *
from
    unioned
    -- order by _accessed_at desc, name
    
