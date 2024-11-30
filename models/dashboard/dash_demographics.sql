with
    get_demographics as (
        select
            member_name,
            parliament,
            party as member_party,
            member_constituency,
            member_ethnicity,
            gender,
            member_birth_year,
            extract(year from first_parl_date) - member_birth_year as year_age_entered
        from {{ ref("dim_members") }}
        left join
            (
                select
                    member_name,
                    parliament,
                    member_constituency,
                    min(`date`) as first_parl_date
                from {{ ref("mart_attendance") }}
                where member_constituency is not null
                group by member_name, parliament, member_constituency
            ) using (member_name)
        order by member_name, parliament
    )
select *
from get_demographics
