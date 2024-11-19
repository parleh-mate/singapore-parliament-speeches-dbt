with get_demographics as(SELECT member_name, parliament, party as member_party, member_constituency, member_ethnicity, gender, member_birth_year, extract(year from first_parl_date) - member_birth_year as year_age_entered
                    FROM `singapore-parliament-speeches.prod_dim.dim_members`
                    left join (SELECT member_name, parliament, member_constituency, min(`date`) as first_parl_date
                        FROM `singapore-parliament-speeches.prod_mart.mart_attendance`
                        where member_constituency is not null
                        group by member_name, parliament, member_constituency)
                    using (member_name)
                    order by member_name, parliament
)
select *
from get_demographics