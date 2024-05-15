with
    records_names as (
        select
            name as original_name,
            email,
            position as latest_position,
            department_url,
            ministry_name,
            _accessed_at as information_accurate_as_at
        from {{ ref("stg_dir_records_names") }}
        where is_latest = true
    ),

    names_mapping as (
        select extracted_name as name, name as original_name
        from {{ ref("stg_dir_preprocess_names_mapping") }}
    ),

    postfixes as (
        select
            extracted_name as name, postfix, effective_from as postfix_effective_since
        from {{ ref("stg_dir_preprocess_postfixes_history") }}
        where is_latest = true
    ),

    prefixes as (
        select extracted_name as name, prefix, effective_from as prefix_effective_since
        from {{ ref("stg_dir_preprocess_prefixes_history") }}
        where is_latest = true
    ),

    joined as (
        select
            prefixes.prefix,
            names_mapping.name,
            postfixes.postfix,
            array_agg(
                distinct coalesce(records_names.original_name, '')
            ) as original_names,
            count(
                distinct coalesce(records_names.original_name, '')
            ) as count_original_names_variations,
            records_names.email,
            array_agg(
                struct(latest_position, department_url, ministry_name)
            ) as latest_positions,
            count(
                distinct concat(latest_position, department_url, ministry_name)
            ) as count_latest_positions,
            min(information_accurate_as_at) as information_accurate_as_at,

            prefixes.prefix_effective_since,
            postfixes.postfix_effective_since,

        from names_mapping
        left join
            records_names on records_names.original_name = names_mapping.original_name
        left join prefixes on names_mapping.name = prefixes.name
        left join postfixes on names_mapping.name = postfixes.name
        group by all

    ),

    gendered_prefixes as (
        select
            *,
            case
                when replace(lower(prefix), '.', '') in ('mr')
                then 'M'
                when
                    replace(lower(prefix), '.', '')
                    in ('ms', 'mdm', 'miss', 'mrs', 'assoc prof (ms)')
                then 'F'
            end as predicted_gender
        from joined
    )

select *
from gendered_prefixes
where count_latest_positions > 0
