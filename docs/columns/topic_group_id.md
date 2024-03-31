{% docs topic_group_id %}

Corresponds to the topic group number (ID) which is the result of a topic modelling.
For the list of topics, use the following query:

```sql
select
    topic as topic_group_id,
    topic_summary as topic_group_name,
    top_n_words
from `singapore-parliament-speeches.topic_modelling.topic_names_19_nmf_20240331`
order by topic_group_id
```

{% enddocs %}
