# Structured Datasets for Singapore's Parliament Speeches
This project aims to make parliament speeches from Singapore's Parliament Hansard structured and accessible. 

A structured format is an enabler. There are applications in computational linguistic analysis, classification, and political science *(Dritsa et. al., 2022)*. Further empirical research on parliamentary discourse and its wider societal impact in recent times is ever more important, given the decisive role of parlimanets and their rapidly changing relations with the public and media *(Erjavec et. al., 2023)*.

This effort addresses the lack of a centralised dataset for Singapore's parliamentary data analysis. *Rauh et. al. (2022)* observed that while more and more political text is available online in principle, bringing the various, often only rather loosely structured sources into a machine-readable format that is readily amenable to automated analysis still presents a major hurdle. Therefore, this initiative seeks to overcome that hurdle.

## Disclaimer

Please note that this is an entirely independent effort, and this initiative is by no means affiliated with the Singapore Parliament nor Singapore Government.

While best efforts are made to ensure the information is accurate, there may be inevitable parsing errors. Please use the information here with caution and check the underlying data.

# This repository

This repository contains code for the data modelling which performs downstream modelling from the raw data which was generated from the [earlier data pipeline](https://github.com/jeremychia/singapore-parliament-speeches/).

Please refer to the [dbt Documentation](https://jeremychia.github.io/singapore-parliament-speeches-dbt/#!/overview), which contains information on the columns available and their descriptions. This was created with the help of [this article](https://medium.com/dbt-local-taiwan/host-dbt-documentation-site-with-github-pages-in-5-minutes-7b80e8b62feb).

The main data product(s) intended for use is

| model | description |
|-------|-------------|
|mart_attendance|By member, by sitting date, whether the member attended the parliamentary sitting or not. This is supplemented with information about the member and sitting.|
|mart_speeches|Each row represents one paragraph of text, based on the hansard, during the parliamentary sitting. This text corresponds to a speech (or part of a speech) made by a Member of Parliament on a given topic. This is supplemented with information about the topic, the sitting, and the member.|

An example of how this dataset is being used is in this [Looker Studio dashboard](https://lookerstudio.google.com/s/qYJulld3Ss8) to show overall attendance.

The services used in this repository are:

* [dbt Cloud](https://cloud.getdbt.com/) (access required)
* [Google BigQuery](https://console.cloud.google.com/bigquery?project=singapore-parliament-speeches&supportedpurview=project) (access required)

# How to contribute

If you are interested to contribute, please reach out to jeremyjchia@gmail.com. 

# References
* *Dritsa, K., Thoma, A., Pavlopoulos, I., & Louridas, P. (2022). A Greek Parliament Proceedings Dataset for Computational Linguistics and Political Analysis. Advances in Neural Information Processing Systems, 35, 28874-28888.*
* *Erjavec, T., Ogrodniczuk, M., Osenova, P., Ljubešić, N., Simov, K., Pančur, A., ... & Fišer, D. (2023). The ParlaMint corpora of parliamentary proceedings. Language resources and evaluation, 57(1), 415-448.*
* *Rauh, C., & Schwalbach, J. (2020). The ParlSpeech V2 data set: Full-text corpora of 6.3 million parliamentary speeches in the key legislative chambers of nine representative democracies.*
