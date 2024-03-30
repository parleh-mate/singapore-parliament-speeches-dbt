{% docs topic_group_name %}

Corresponds to the topic group's name, which is derived, using an LLM, from the top n words.
For the list of topics, use the following query:

```sql
select
    topic as topic_group_id,
    topic_summary as topic_group_name,
    top_n_words
from `singapore-parliament-speeches.topic_modelling.topic_names_16_nmf_20240330`
order by topic_group_id
```

{% enddocs %}
