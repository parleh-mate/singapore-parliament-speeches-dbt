import pandas_gbq
import os
import numpy as np
import pandas as pd
import random
from openai import OpenAI
from datetime import date
import jsonlines
import json
from google.cloud import bigquery

# gcp key

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "token/gcp_token.json"

# gpt key

os.environ["OPENAI_API_KEY"] = open("token/gpt_api_token.txt", 'r').readlines()[0]

gpt_client = OpenAI()

# Collect results from previous batch run

batch_check_df = pandas_gbq.read_gbq("""
                    SELECT *
                    FROM `singapore-parliament-speeches.prod_mart.mart_speeches_summaries`
                         WHERE status = 'executing'
                    """)

batch_check_id = batch_check_df.gpt_batch_id[0]

batch_meta = gpt_client.batches.retrieve(batch_check_id)

if batch_meta.status=="completed":
    file_response = gpt_client.files.content(batch_meta.output_file_id)
    text_response = file_response.read().decode('utf-8')
    text_dict = [json.loads(obj) for obj in text_response.splitlines()]

    data = []
    for i in text_dict:
        speech_id = i['custom_id']
        speech_summary = i['response']['body']['choices'][0]['message']['content']        
        prompt_tokens = i['response']['body']['usage']['prompt_tokens']
        output_tokens = i['response']['body']['usage']['completion_tokens']
        data.append((speech_id, speech_summary, prompt_tokens, output_tokens))
    
    speech_summaries = pd.DataFrame(data, columns=['speech_id', 'speech_summary', 'prompt_tokens', 'output_tokens'])

    batch_check_to_merge = batch_check_df.drop(['speech_summary',
                                                'prompt_tokens',
                                                'output_tokens'], 
                                                axis = 1)

    summary_df = pd.merge(batch_check_to_merge,
                          speech_summaries,
                          how = 'left',
                          on = 'speech_id')
    
    summary_df['status'] = 'completed'

    # now append to existing table and remove executing rows

    pandas_gbq.to_gbq(dataframe = summary_df, 
                  destination_table = "prod_mart.mart_speeches_summaries",
                  if_exists = 'append')
    
    # now delete executing entries

    gbq_client = bigquery.Client()
    gbq_client.query("""
                     DELETE
                     FROM `singapore-parliament-speeches.prod_mart.mart_speeches_summaries`
                     WHERE status = 'executing'
                     """)
    
    
# get new speeches and get summaries

# this bit here needs to be modified to read only new data coming in, currently it reads all data

df = pandas_gbq.read_gbq("""
                    SELECT  *
                    FROM `singapore-parliament-speeches.prod_mart.mart_speeches`
                         WHERE member_name != ''
                         AND member_name != 'Speaker'
                    """)

# get topic ministry

# this can and probably should be done at raw stage

def get_maj_ministry(ministries, out = 'ministry'):
    counts = [list(ministries).count(i) for i in ministries]
    ministry = np.array(list(ministries))[counts==np.max(counts)]

    # take majority ministry; if tie randomly sample
    if len(ministry)==1:
        min_name = ministry[0]
    min_name = random.sample(list(ministry), k = 1)[0]
    prop = list(ministries).count(min_name)/len(ministries)
    if out=='ministry':
        return min_name
    return prop

df_groupby = df.groupby('topic_id')['ministry_addressed']

df['topic_ministry'] = df_groupby.transform(get_maj_ministry)

df['topic_ministry_prop'] = df_groupby.transform(get_maj_ministry, out = 'prop')

# filter out

reduced_df = df.query('~topic_type_name.str.contains(r"Bill Introduced|Correction by Written Statements") and ~is_primary_question')

# hard code upper and lower bounds
# bounds are hard coded now; using percentiles means these change with new speeches coming in

upper_bound = 2000
lower_bound = 70

reduced_df = reduced_df.query(f'count_speeches_words<{upper_bound} and count_speeches_words>{lower_bound}')

# generate batch info

model = "gpt-4o-mini"
topics = ["Law", "Economic Development", "Public Health", 
          "Military Defense", "Housing and Urban Development", 
          "Labour", "Public Transport", "Foreign Policy", 
          "Education", "Family and social welfare", "Environment and Climate", ""]
topics = ", ".join(topics)

prompt = f"You are a helpful assistant who will help to summarize speeches made in the Singapore parliament. Summaries should be short and concise, between 3-4 sentences and ideally less than 80 words, and be written from the perspective of the speaker. Opinions and arguments should be stated directly and depersonalized rather than written like 'I believe...', 'I am here to...'. For example, 'Everyone has a right to healthcare' is preferred to 'I believe that everyone has a right to healthcare'. I also want you to label a topic for each speech from one of these categories: {topics}. Some speeches are clustered together as responses to the same parliamentary question. For these speeches, I will provide the ministry to which the question is addressed, in case this helps with the labeling. Speeches may digress from the initial question and so you should use discretion when deciding if the targeted ministry is informative. In your output, return it in the format " + "'Summary:{summary here}, Topic: {topic here}.'"

# now write batch job

json_list = []
for ind,row in reduced_df[:10].iterrows():
    json_list.append({"custom_id": row.speech_id, 
      "method": "POST",
      "url": "/v1/chat/completions",
      "body": {"model": model, 
               "messages": [{"role": "system", 
                             "content": prompt},
                             {"role": "user", 
                              "content": row.speech_text}],
                              "max_tokens": 3000
      }})

with jsonlines.open('batch_summary.jsonl', 'w') as writer:
    writer.write_all(json_list)

# run batch job

batch_input_file = gpt_client.files.create(
  file=open("batch_summary.jsonl", "rb"),
  purpose="batch"
)

batch_file_id = batch_input_file.id

batch_meta = gpt_client.batches.create(
    input_file_id=batch_file_id,
    endpoint="/v1/chat/completions",
    completion_window="24h",
    metadata={
      "description": "singapore parliamentary speech summary batch job"
    }
)

gpt_batch_id = batch_meta.id

batch = batch_check_df.batch[0] + 1
batch_date = str(date.today())

batch_df = reduced_df[:10].filter(['speech_id',
                        'date',
                        'parliament',
                        'member_name',
                        'member_constituency',
                        'count_speeches_words',
                        'speech_text',
                        ])

batch_df = batch_df.assign(speech_summary = "",
                           batch = batch,
                           batch_date = batch_date,
                           prompt = prompt,
                           model = model,
                           prompt_tokens = 0,
                           output_tokens = 0,
                           gpt_batch_id = gpt_batch_id,
                           status = "executing")

pandas_gbq.to_gbq(dataframe = batch_df, 
                  destination_table = "prod_mart.mart_speeches_summaries",
                  if_exists = 'append')