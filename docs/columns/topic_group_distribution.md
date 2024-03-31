{% docs topic_group_distribution %}

When topic modelling is done, probabilities are assigned to each topic to ascertain what is the likelihood the topic belongs to a category.
Topic group distribution refers to the probability that the given topic belongs to the highest group.

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
