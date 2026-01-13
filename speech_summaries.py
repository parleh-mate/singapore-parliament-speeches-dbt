import os
import pandas as pd
from openai import OpenAI
from datetime import date
import jsonlines
import json
from google.cloud import bigquery
import time

# gcp key

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "token/gcp_token.json"

# gpt key

os.environ["OPENAI_API_KEY"] = open("token/gpt_api_token.txt", 'r').readlines()[0]

gpt_client = OpenAI()

# GBQ client
gbq_client = bigquery.Client()

# Collect results from previous batch run

pause_time = 900

while True:
    print("starting loop")

    table_id = "singapore-parliament-speeches.prod_dim.dim_speech_summaries"

    job = gbq_client.query(f"""
                        SELECT *
                        FROM `{table_id}`
                            WHERE status = 'in_progress'
                        """)

    result = job.result()
    batch_check_df = result.to_dataframe()

    if len(batch_check_df)!=0:
        gpt_batch_id = batch_check_df.gpt_batch_id[0]
        batch_id = batch_check_df.batch_id[0]

        batch_meta = gpt_client.batches.retrieve(gpt_batch_id)

        print("checking status of previous batch")

        if batch_meta.status=="completed":
            print("previous batch completed")
            file_response = gpt_client.files.content(batch_meta.output_file_id)
            text_response = file_response.read().decode('utf-8')
            text_dict = [json.loads(obj) for obj in text_response.splitlines()]

            data = []
            for i in text_dict:
                try:
                    speech_id = i['custom_id']
                    output = eval(i['response']['body']['choices'][0]['message']['content'])
                    speech_summary = output['Summary']
                    topic_assigned = output['Topic']
                    data.append((speech_id, speech_summary, topic_assigned))
                except:
                    pass
            
            speech_summaries = pd.DataFrame(data, columns=['speech_id', 'speech_summary', 'topic_assigned'])
            speech_summaries['batch_id'] = batch_id
            speech_summaries['gpt_batch_id'] = gpt_batch_id

            # now create or append to new table and change executing rows

            speech_summaries_table_id = "singapore-parliament-speeches.prod_mart.mart_speech_summaries"

            try:
                gbq_client.get_table(speech_summaries_table_id)  # Check if table exists
                print(f"Table {speech_summaries_table_id} exists, appending data.")
                # If the table exists, configure the load job to append data
                job_config = bigquery.LoadJobConfig(
                    write_disposition = bigquery.WriteDisposition.WRITE_APPEND  # Append to existing table
                )
            except Exception as e:
                print(f"Table {table_id} does not exist, creating table.")
                # If the table does not exist, create it and upload data
                table = bigquery.Table(speech_summaries_table_id)
                gbq_client.create_table(table)  # Create the table
                job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_EMPTY  # Create table if empty
                )


            job = gbq_client.load_table_from_dataframe(speech_summaries, 
                                                    speech_summaries_table_id, 
                                                    job_config = job_config)
            job.result()
            
            # now change executing entries

            job = gbq_client.query(f"""
                            UPDATE `{table_id}`
                            SET status = 'completed'
                            WHERE status = 'in_progress'
                            """)
            job.result()

        else:
            print("Previous batch job not completed, restarting loop")
            print(f"starting {pause_time}s pause")
            time.sleep(pause_time)
            print("pause ended")
            continue        
        
    # get new speeches and get summaries
    # this bit here needs to be modified to read only new data coming in, currently it reads all data

    # hard code upper and lower bounds
    # bounds are hard coded now; using percentiles means these change with new speeches coming in

    print("Creating new batch")

    upper_bound = 2000
    lower_bound = 70
    batch_size = 1000

    job = gbq_client.query(f"""
                        SELECT  *
                        FROM `singapore-parliament-speeches.prod_mart.mart_speeches`
                        WHERE speech_id not in (select speech_id from `singapore-parliament-speeches.prod_mart.mart_speech_summaries`)
                        AND topic_type_name not like "%Correction by Written Statements%"
                        AND topic_type_name not like "%Bill Introduced%"
                        AND count_speeches_words<{upper_bound} 
                        AND count_speeches_words>{lower_bound}
                        AND member_name != ''
                        AND member_name != 'Speaker'
                        LIMIT {batch_size}
                        """)

    result = job.result()
    df = result.to_dataframe()

    # get pre deterimed topics
    job = gbq_client.query("""
                        SELECT DISTINCT topic_name
                        FROM `singapore-parliament-speeches.prod_fact.fact_topics`
                            WHERE include and
                            topic_name not like 'Procedural'
                        """)

    result = job.result()
    topics = list(result.to_dataframe().topic_name)

    topics = ", ".join(topics)

    word_limit = 150

    system_message = f"""You will be provided with a speech from the Singapore parliament. You are a helpful assistant who will summarize this speech in no more than ~{word_limit} words and label it with a topic. 
    """

    output_summary_description = f"""A concise summary of the speech of no more than {word_limit} words. Sometimes speeches are long and unsubstantive. I will provide a step-by-step process to guide your summarization:

    1. Extract all the key policy points in the speech and omit anything that is unsubstantive. Unsubstantive items include reiteration of someone else's speech, parliamentary decorum (thanking another member or the speaker), and procedural points. 

    2. Rank the key points according to this hierarchy, where 1 indicates highest priority: 1) specific policy proposal or recommendation 2) advocacy or highlighting an issue without mentioning specific remedies 3) miscellaneous commentary or anecdotes

    3. Summarize the speech and give priority to items higher up in the hierarchy, and if need be generously omit things that cannot fit into the word limit.

    Note that {word_limit} words is only a limit and a summary can be a lot shorter if the speech is short and unsubstantive. As a rule of thumb a summary cannot be longer than the speech it is summarizing.

    Adhere strictly to the following writing style: 

    1. Use concise language, avoiding tautology. 

    2. Write in the present tense passive voice like you would an objective report, avoiding pronouns if possible. For example, 'Singapore has longstanding partnerships with China' is preferred to 'I highlight Singapore's longstanding partnerships with China'.

    3. If you have to use pronouns, strictly avoid using the first-person and write in the third-person instead. For example, 'The speaker/speech emphasizes the need to support SMEs during pandemic recovery' is preferred to 'We need to support SMEs during pandemic recovery'.

    4. Do not expand acronyms, just leave them as they are.
    """

    output_topic_description = f"""The topic of the speech chosen ONLY from one from these topics: {topics}. Some speeches are responses to a parliamentary question. In this case, I will provide the ministry(s) to which the question is addressed to help you with the labelling. In the case that there are two, the first ministry will be the ministry that is mentioned the most, and the second one addressed second-most. Use this information wisely since speeches may digress from the initial question.
    """

    response_format = {"type": "json_schema", "json_schema": {"name": "response", "strict": True, "schema": {"type": "object", "properties": {"Summary": {"type": "string", "description": output_summary_description}, "Topic": {"type": "string", "description": output_topic_description}}, "required": ["Summary", "Topic"], "additionalProperties": False}}}

    # now write batch job

    model = "gpt-4o-mini"

    json_list = []
    for ind,row in df.iterrows():

        ministries = [i for i in row.filter(['ministry_addressed_primary', 
                                                            'ministry_addressed_secondary']) if i is not None]
        
        mins_addressed = ','.join(ministries)

        input = f"Speech: [{row.speech_text}], [Ministries addressed: {mins_addressed}]"    

        json_list.append({"custom_id": row.speech_id, 
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {"model": model, 
                "messages": [{"role": "system", 
                                "content": system_message},
                                {"role": "user", 
                                "content": input}],
                                "max_tokens": 3000,
                                "response_format": response_format
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

    batch_id = batch_check_df.batch_id.max() + 1
    batch_date = date.today()

    dim_speech_summaries = pd.DataFrame({"batch_id": [batch_id],
                                        "gpt_batch_id": [gpt_batch_id],
                                        "model": [model],
                                        "batch_date": [batch_date],
                                        "system_message": [system_message],
                                        "output_summary_description": [output_summary_description],
                                        "output_topic_description": [output_topic_description],
                                        "word_limit": [word_limit],
                                        "status": ["in_progress"]})

    schema = [
        bigquery.SchemaField("batch_id", "INTEGER"),
        bigquery.SchemaField("gpt_batch_id", "STRING"),
        bigquery.SchemaField("model", "STRING"),
        bigquery.SchemaField("batch_date", "DATE"),
        bigquery.SchemaField("system_message", "STRING"),
        bigquery.SchemaField("output_summary_description", "STRING"),
        bigquery.SchemaField("output_topic_description", "STRING"),
        bigquery.SchemaField("word_limit", "INTEGER"),
        bigquery.SchemaField("status", "STRING")
    ]

    try:
        gbq_client.get_table(table_id)  # Check if table exists
        print(f"Table {table_id} exists, appending data.")
        # If the table exists, configure the load job to append data
        job_config = bigquery.LoadJobConfig(
            write_disposition = bigquery.WriteDisposition.WRITE_APPEND  # Append to existing table
        )
    except Exception as e:
        print(f"Table {table_id} does not exist, creating table.")
        # If the table does not exist, create it and upload data
        table = bigquery.Table(table_id, schema=schema)
        gbq_client.create_table(table)  # Create the table
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_EMPTY  # Create table if empty
        )

    job = gbq_client.load_table_from_dataframe(dim_speech_summaries, table_id, job_config = job_config)
    job.result()

    print("Batch uploaded, end of loop")
    print(f"Start {pause_time}s pause")
    time.sleep(pause_time)
    print("pause ended, restarting loop")