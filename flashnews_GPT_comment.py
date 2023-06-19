import os
import numpy as np
import pandas as pd
from datetime import datetime
import time
import openai

openai.organization = "org-CZ3SLwuVO3gHVVngRow4srBd"
openai.api_key = "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"
openai.Model.list()

import backoff  # for exponential backoff

@backoff.on_exception(backoff.expo, openai.error.RateLimitError)
def completions_with_backoff(**kwargs):
    return openai.Completion.create(**kwargs)


##### Functions ######

def GetSentiment_v1(prompt_text):
    the_response = completions_with_backoff(
        model="text-davinci-003",
        prompt=prompt_text,
        temperature=0,
        max_tokens=8,
        top_p=1.0,
        frequency_penalty=0.0,
        presence_penalty=0.0
    )

    result = the_response.choices[0].text.replace('\n', '')
    return (result)

with open("my_result_jin10.txt", "w") as f:
    f.write("Begin test\n")

results = pd.DataFrame(columns=['GPT_Res'], index = df.index)
for i in range(0, int(len(df)/5)):
    # for i in range(0, 100):
    prompt_text = []
    time_text = []
    for n in range(5):
        news_text = content.content.iloc[n + 5*i,]
        time_text.append(n + 5*i)
        instruction_text = 'Pretend you are a trader that trades on ferrous products. Given the news on ferrous products below in ' \
                           'Chinese, what is the short term outlook for Chinese Ferrous products. Give a one word answer of either "YES" if' \
                           ' the sentiment is positive, "NO" if the sentiment is negative or "NEUTRAL" if unsure or neutral.  \n'
        try:
            prompt_text.append(instruction_text + '\n\n' + news_text)
        except:
            if np.isnan(news_text):
                prompt_text.append('Pls return Not News')
            else:
                prompt_text.append('Pls return Error')
    # Give prompt to GPT
    try:
        result = GetSentiment_v1_batching(prompt_text)
    except:
        result = []
        for prompt_ in prompt_text:
            try:
                result_ = GetSentiment_v1_batching(prompt_)
                result.append(result_[0])
            except:
                txt1, txt2 = prompt_.split('\n\n\n')
                txt2 = txt2.split('<br/>')
                result_ = None
                for txt in txt2:
                    result_sub =  GetSentiment_v1_batching([instruction_text + '\n\n' + txt])
                    if result_ == None:
                        result_ = result_sub
                    else:
                        result_[0]['text'] += ('|'+result_sub[0]['text'])
                result.append(result_[0])
                    
    result = pd.DataFrame(result).text.str.replace('\n', '')
    result.index = time_text
    try:
        results.loc[time_text,'GPT_Res'] = result
    except:
        results.loc[time_text,'GPT_Res'] = result.values.tolist()

    time.sleep(5)
   
    results.to_html('jin10_results.html')

    # Print & Save
    if i % 100 == 0:
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        print("Current index: ", i, ".", current_time)
        pd.DataFrame(results).to_csv("test_jin10.csv")