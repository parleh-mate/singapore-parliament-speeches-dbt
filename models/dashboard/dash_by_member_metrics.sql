WITH cte_speeches AS
  (SELECT *,
          count_speeches - count_pri_questions AS count_only_speeches
   FROM `singapore-parliament-speeches.prod_agg.agg_speech_metrics_by_member`),
     cte_readability AS
  (SELECT member_name,
          parliament,
          206.835 - 1.015*(count_speeches_words / count_speeches_sentences) - 84.6*(count_speeches_syllables/count_speeches_words) AS readability_score
   FROM `singapore-parliament-speeches.prod_mart.mart_speeches`
   WHERE NOT is_vernacular_speech 
     AND NOT is_primary_question
     AND count_speeches_words>0
     AND member_constituency IS NOT NULL
     AND 'Speaker' NOT IN UNNEST(member_appointments)),
     by_parl_speeches AS
  (SELECT member_name,
          CAST(parliament AS STRING) AS parliament,
          member_party,
          member_constituency,
          ROUND(CASE
                    WHEN SUM(count_sittings_present) = 0 THEN 0
                    ELSE SUM(count_pri_questions) / SUM(count_sittings_present)
                END, 2) AS questions_per_sitting,
          CAST(CASE
                   WHEN SUM(count_only_speeches) = 0 THEN 0
                   ELSE SUM(count_words) / SUM(count_only_speeches)
               END AS INTEGER) AS words_per_speech,
          ROUND(CASE
                    WHEN SUM(count_sittings_present) = 0 THEN 0
                    ELSE SUM(count_only_speeches) / SUM(count_sittings_present)
                END, 2) AS speeches_per_sitting
   FROM cte_speeches
   GROUP BY parliament,
            member_party,
            member_constituency,
            member_name),
     by_parl_join_readability AS
  (SELECT *
   FROM by_parl_speeches
   LEFT JOIN
     (SELECT member_name,
             CAST(parliament AS STRING) AS parliament,
             round(avg(readability_score), 1) AS readability_score
      FROM cte_readability
      GROUP BY member_name,
               parliament) USING (member_name,
                                  parliament)),
     all_parl_speeches AS
  (SELECT member_name,
          'All' AS parliament,
          member_party,
          'All' AS member_constituency,
          ROUND(CASE
                    WHEN SUM(count_sittings_present) = 0 THEN 0
                    ELSE SUM(count_pri_questions) / SUM(count_sittings_present)
                END, 2) AS questions_per_sitting,
          CAST(CASE
                   WHEN SUM(count_only_speeches) = 0 THEN 0
                   ELSE SUM(count_words) / SUM(count_only_speeches)
               END AS INTEGER) AS words_per_speech,
          ROUND(CASE
                    WHEN SUM(count_sittings_present) = 0 THEN 0
                    ELSE SUM(count_only_speeches) / SUM(count_sittings_present)
                END, 2) AS speeches_per_sitting
   FROM cte_speeches
   WHERE member_constituency IS NOT NULL
   GROUP BY member_name,
            member_party),
     all_parl_join_readability AS
  (SELECT *
   FROM all_parl_speeches
   LEFT JOIN
     (SELECT member_name,
             'All' AS parliament,
             round(avg(readability_score), 1) AS readability_score
      FROM cte_readability
      GROUP BY member_name) USING (member_name,
                                   parliament)),
     speech_agg AS
  (SELECT *
   FROM by_parl_join_readability
   UNION ALL SELECT *
   FROM all_parl_join_readability),
     by_parl AS
  (SELECT member_name,
          cast(parliament AS STRING) AS parliament,
          member_party,
          member_constituency,
          sum(count_sittings_total) AS sittings_total,
          sum(count_sittings_present) AS sittings_present,
          sum(count_sittings_spoken) AS sittings_spoken
   FROM `singapore-parliament-speeches.prod_agg.agg_speech_metrics_by_member`
   GROUP BY member_name,
            parliament,
            member_party,
            member_constituency),
     all_parl AS
  (SELECT member_name,
          'All' AS parliament,
          member_party,
          'All' AS member_constituency,
          sum(count_sittings_total) AS sittings_total,
          sum(count_sittings_present) AS sittings_present,
          sum(count_sittings_spoken) AS sittings_spoken
   FROM `singapore-parliament-speeches.prod_agg.agg_speech_metrics_by_member`
   GROUP BY member_name,
            member_party),
     participation AS
  (SELECT *
   FROM by_parl
   UNION ALL SELECT *
   FROM all_parl),
   
   round_participation as (SELECT member_name,
       parliament,
       member_party,
       member_constituency,
       round(100*sittings_present/nullif(sittings_total, 0), 1) AS attendance,
       round(100*sittings_spoken/nullif(sittings_present, 0), 1) AS participation
FROM participation
   ),
   join_speech_and_participation as (
    select *
    from speech_agg
    left join round_participation
    using (member_name, member_party, member_constituency, parliament)
   )

SELECT *
FROM join_speech_and_participation
WHERE member_constituency IS NOT NULL