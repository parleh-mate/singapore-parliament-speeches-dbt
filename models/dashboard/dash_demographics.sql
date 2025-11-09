with
    get_demographics as (
        select
            members.member_name,
            constituency.parliament,
            party.party,
            constituency.member_constituency,
            members.member_ethnicity,
            members.gender,
            members.member_birth_year,
            extract(year from constituency.first_parl_date) - members.member_birth_year as year_age_entered
        from {{ ref("dim_members") }} as members
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
            ) as constituency using (member_name)
        left join {{ ref("stg_gsheet_member_party") }} as party
        on members.member_name = party.member_name
        and constituency.parliament = party.parliament
        order by member_name, parliament
    )
select *
from get_demographics
