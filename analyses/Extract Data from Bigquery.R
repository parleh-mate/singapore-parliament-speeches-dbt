# Installation and Authorisation

if (!require("bigrquery", quietly = TRUE)) {
  # If not installed, install it
  install.packages("bigrquery")
}
library(bigrquery)
library(dplyr)
library(ggplot2)

# Access to the project is required.
# This ensure access.

bq_auth()

# For more information, see the Github repository:
# https://github.com/jeremychia/singapore-parliament-speeches-dbt/

project_id <- "singapore-parliament-speeches"

# Add filters in the SQL query where necessary.
# For e.g. where date = '2024-03-04' returns speeches made on that date.
# Writing filters in the SQL query helps to reduce the size of the df,
# improving performance in R.

sql_query <- "
select
  *
from
  prod_mart.mart_speeches
where
  date = '2024-03-04'
"

# Extract

query_job <- bq_project_query(project_id, sql_query) # Run the query
df <- bq_table_download(query_job,
                        page_size = 1e3)

df %>% 
  names

df %>% 
  filter(date == "2024-03-04") %>% # unnecessary if filtered in SQL
  arrange(speech_id) %>% 
  select(speech_id,
         member_name,
         member_party,
         speech_text,
         count_speeches_words,
         topic_title)

# Analyse

party_colors <- c("NMP" = "grey",
                  "PAP" = "red",
                  "WP"  = "blue",
                  "PSP" = "yellow")

df %>%
  filter(member_party != "NA") %>% 
  ggplot(aes(x = member_party, y = count_speeches_words, fill = member_party)) +
  geom_boxplot() +
  labs(title = "Boxplot of Count of Speeches by Member Party",
       x = "Member Party",
       y = "Count of Speeches Words") +
  scale_fill_manual(values = party_colors) +
  theme_minimal() + 
  theme(panel.grid = element_blank())

