with
    records_departments as (
        select
            parent_name,
            department_name,
            department_url,
            ministry_name,
            _accessed_at,
            is_latest
        from {{ ref("stg_dir_records_departments") }}
    ),

    aggregate_departments as (
        select
            max_by(department_name, _accessed_at) as latest_department_name,
            array_agg(distinct department_name) as department_names,
            count(distinct department_name) as count_department_names,
            parent_name,
            ministry_name,
            min(_accessed_at) as effective_from,
            max(_accessed_at) as effective_to,
            max(is_latest) as is_active,
            department_url
        from records_departments
        group by all
    )

select *
from aggregate_departments
