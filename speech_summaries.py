import pandas_gbq
import os
import pandas as pd
import anthropic
import os
from openai import OpenAI
from datetime import datetime

# first import from GBQ

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "token/gcp_token.json"

# we subsample small number of speeches

speeches_df = pandas_gbq.read_gbq("""
                    SELECT  *
                    FROM `singapore-parliament-speeches.prod_mart.mart_speeches_filtered`
                                  ORDER BY RAND() 
                                  LIMIT 10
                    """)

# we test our speech summaries on a few models, namely Claude opus, sonnet 3.5, GPT 4o, and GPT 4o-mini

# set api keys
os.environ["ANTHROPIC_API_KEY"] = open("token/claude_api_token.txt", 'r').readlines()[0]

os.environ["OPENAI_API_KEY"] = open("token/gpt_api_token.txt", 'r').readlines()[0]

# calculate costs

def cost_calculation(input_tokens, output_tokens, incost_pmt, outcost_pmt):
    input_cost = input_tokens*incost_pmt/1000000
    output_cost = output_tokens*outcost_pmt/1000000

    return input_cost + output_cost

def get_costs(n_words, prompt_len, model, 
              input_tokens, output_tokens):

    if model=="claude-3-5-sonnet-20240620":
        incost_pmt, outcost_pmt = 3, 15
    elif model=="claude-3-opus-20240229":
        incost_pmt, outcost_pmt = 15, 75
    elif model=="gpt-4o":
        incost_pmt, outcost_pmt = 5, 15
    elif model=="gpt-4o-mini":
        incost_pmt, outcost_pmt = 0.15, 0.6

    # first get estimated cost
    # approx 1 word = 4/3 tokens
    # assuming a 2-3 sentence output = 60 tokens
    est_input_tokens = (n_words+prompt_len) * (4/3)
    est_cost = cost_calculation(est_input_tokens, 60, incost_pmt, outcost_pmt)

    actual_cost = cost_calculation(input_tokens, output_tokens, incost_pmt, outcost_pmt)

    return {"est_cost": est_cost,
            "actual_cost": actual_cost}

def get_summaries(speech_dat):
    models = ["claude-3-5-sonnet-20240620",
              "claude-3-opus-20240229",
              "gpt-4o",
              "gpt-4o-mini"]
    
    speech = speech_dat.speech_text.iloc[0]
    prompt = "You are a helpful assistant who will help to summarize speechs made in the Singapore parliament. Summaries should be short and concise, between 2-3 sentences and ideally less than 60 words, and be written in the perspective of the speaker. Opinions and arguments should be stated immediately rather than written like 'I believe...', 'I am here to...'."
    df_list = []    
    for i in models:
        if "claude" in i:

            message = claude_client.messages.create(
                model=i,
                max_tokens=4096,
                temperature = 1,
                system=prompt,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": speech
                            }
                        ]
                    }
                ]
            )
            speech_sum = message.content[0].text
            input_tokens = message.usage.input_tokens
            output_tokens = message.usage.output_tokens

        else:         
            # else use openai

            completion = gpt_client.chat.completions.create(
            model=i,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": speech}
            ]
            )

            speech_sum = completion.choices[0].message.content
            input_tokens = completion.usage.prompt_tokens
            output_tokens = completion.usage.completion_tokens

        # get costs
        costs = get_costs(n_words = speech_dat.count_speeches_words,
                              prompt_len = len(prompt.split(" ")), 
                              model = i, 
                              input_tokens = input_tokens, 
                              output_tokens = output_tokens)

        out_df = speech_dat.assign(model = i,
                                   speech_summary = speech_sum,
                                   est_cost = costs['est_cost'],
                                   actual_cost = costs['actual_cost'])
        
        df_list.append(out_df)
    
    return pd.concat(df_list)

# initialize clients

claude_client = anthropic.Anthropic()
gpt_client = OpenAI()

df_list = []
for ind,row in speeches_df.iterrows():
    output_df = get_summaries(row.to_frame().T)
    output_df = output_df.reset_index().rename(columns = {"index": "id"})
    df_list.append(output_df)

final_df = pd.concat(df_list)

final_df['date'] = [datetime.strptime(i, '%Y-%m-%d %H:%M:%S.%f000').date() for i in final_df.date]

schema = [
    {'name': 'id', 'type': 'INTEGER'},                       
    {'name': 'date', 'type': 'DATE'},                      
    {'name': 'parliament', 'type': 'INTEGER'},                
    {'name': 'member_name', 'type': 'STRING'},               
    {'name': 'member_party', 'type': 'STRING'},              
    {'name': 'member_constituency', 'type': 'STRING'},       
    {'name': 'count_speeches_words', 'type': 'INTEGER'},
    {'name': 'speech_text', 'type': 'STRING'},               
    {'name': 'model', 'type': 'STRING'},                     
    {'name': 'speech_summary', 'type': 'STRING'},            
    {'name': 'est_cost', 'type': 'FLOAT'},                  
    {'name': 'actual_cost', 'type': 'FLOAT'}
]

final_df.query('member_name=="Khaw Boon Wan"').speech_summary

pandas_gbq.to_gbq(dataframe = final_df,
                  destination_table = "prod_mart.mart_speeches_summary",
                  if_exists = 'replace',
                  table_schema = schema)




