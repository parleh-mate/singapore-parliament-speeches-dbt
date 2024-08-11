# Quick Start

This guide is meant to help first-time contributors understand how to query information from the 3 main data products of this database.

## Data Products

As a refresher, these are the data products intended for use:

| model | description |
|-------|-------------|
|[`mart_attendance`](https://console.cloud.google.com/bigquery?ws=!1m5!1m4!4m3!1ssingapore-parliament-speeches!2sprod_mart!3smart_attendance)|By member, by sitting date, whether the member attended the parliamentary sitting or not. This is supplemented with information about the member and sitting.|
|[`mart_speeches`](https://console.cloud.google.com/bigquery?ws=!1m5!1m4!4m3!1ssingapore-parliament-speeches!2sprod_mart!3smart_speeches)|Each row represents one paragraph of text, based on the hansard, during the parliamentary sitting. This text corresponds to a speech (or part of a speech) made by a Member of Parliament on a given topic. This is supplemented with information about the topic, the sitting, and the member.|
|[`mart_bills`](https://console.cloud.google.com/bigquery?ws=!1m5!1m4!4m3!1ssingapore-parliament-speeches!2sprod_mart!3smart_bills)|By bill, shows a summary of the bill's passage through parliament.|
|[`dim_members`](https://console.cloud.google.com/bigquery?ws=!1m5!1m4!4m3!1ssingapore-parliament-speeches!2sprod_dim!3sdim_members)|Each row represents information about a specific member.|

## Sample Queries

### For `mart_attendance`

```sql
select *
from `singapore-parliament-speeches.prod_mart.mart_attendance`
```

### For `mart_speeches`

```sql
select *
from `singapore-parliament-speeches.prod_mart.mart_speeches`
```

### For `mart_bills`

```sql
select *
from `singapore-parliament-speeches.prod_mart.mart_bills`
```

### For `dim_members`

```sql
select *
from `singapore-parliament-speeches.prod_dim.dim_members`
```
