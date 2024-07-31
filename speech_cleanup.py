import pandas_gbq
import os
import pandas as pd
import numpy as np
import re

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "token/gcp_token.json"

df = pandas_gbq.read_gbq("""
                    SELECT  *
                    FROM `singapore-parliament-speeches.prod_mart.mart_speeches`
                         WHERE member_name != ''
                         AND member_name != 'Speaker'
                         AND is_primary_question = false
                    """)

reduced_df = df.query('~topic_type_name.str.contains(r"Bill Introduced|Correction by Written Statements")')

perc_95 = np.percentile(reduced_df.count_speeches_words, 95)

filtered_df = reduced_df.query(f'count_speeches_words<{perc_95} and count_speeches_words>70')

filtered_df = filtered_df.assign(is_vernacular = lambda x: x.speech_text.str.contains("Vernacular Speech"))

def extract_language(text):
    match = re.search(r"In (Malay|Mandarin|Tamil)", text)
    return match.group(1) if match else None

filtered_df['vernacular_lang'] = filtered_df['speech_text'].apply(extract_language)

filtered_df['vernacular_lang'] = np.where(filtered_df['is_vernacular'], filtered_df['vernacular_lang'], None)

pandas_gbq.to_gbq(dataframe = filtered_df[['date', 
                                           'parliament',
                                           'member_name',
                                           'member_party',
                                           'member_constituency',
                                           'count_speeches_words',
                                           'speech_text']], 
                  destination_table = "prod_mart.mart_speeches_filtered",
                  if_exists = 'replace')