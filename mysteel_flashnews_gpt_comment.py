import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
import time
import openai
import pytz
tz = pytz.timezone('Asia/Hong_Kong')

openai.organization = "org-CZ3SLwuVO3gHVVngRow4srBd"
openai.api_key = "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"
openai.Model.list()

import datetime as dt_
ssql = sqlite3.connect('/home/quantcargillmetals/FlashNewsSQL.db')
db_mysteel = pd.read_sql('SELECT * FROM MySteel_FlashNews', con = ssql)
ssql.close()
db_mysteel

def no_gpt_comment(df):
    no_gpt =\
        (db_mysteel.GPT_Comment.str.upper().str.contains('NEUTRAL')|\
          db_mysteel.GPT_Comment.str.upper().str.contains('YES')|\
          db_mysteel.GPT_Comment.str.upper().str.contains('NO'))
    return df.loc[~no_gpt]

pre_list = ['分析解读', '钢联调研', '产业监测', '企业调价']
content = no_gpt_comment(db_mysteel).copy()
content = content[content.flashNewsColumnName.apply(lambda f: f in pre_list)]

content.datetime = pd.to_datetime(content.datetime)
content = content.set_index('datetime')
content.sort_index(inplace = True)

content = content.loc[(dt_.datetime.now() - dt_.timedelta(days = 10)).strftime('%Y-%m-%d %H:%M:%S'):\
                        dt_.datetime.now().strftime('%Y-%m-%d %H:%M:%S')].reset_index()

import backoff  # for exponential backoff

@backoff.on_exception(backoff.expo, openai.error.RateLimitError)
def completions_with_backoff(**kwargs):
    return openai.Completion.create(**kwargs)


def GetSentiment_v1_batching(prompt_text):
    the_response = completions_with_backoff(
        model="text-davinci-003",
        prompt=prompt_text,
        temperature=0,
        max_tokens=8,
        top_p=1.0,
        frequency_penalty=0.0,
        presence_penalty=0.0
    )

    result = the_response.choices
    return (result)
instruction_text = 'Pretend you are a trader that trades on ferrous products. Given the news on ferrous products below in ' \
                    'Chinese, what is the short term outlook for Chinese Ferrous products. Give a one word answer of either "YES" if' \
                    ' the sentiment is positive, "NO" if the sentiment is negative or "NEUTRAL" if unsure or neutral.  \n'
        
##### Running GPT Test ######
for i in range(0, int(content.shape[0]/10)):
    prompt_text = []
    time_text = []
    
    for n in range(10):
        news_text = content.flashNewsContent.iloc[n + 10*i]
        time_text.append(n + 10*i)
        
        try:
            prompt_text.append(instruction_text + '\n\n' + news_text + '。')
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
    result.index = content.iloc[time_text,].index
    try:
        content.loc[result.index, 'GPT_Comment'] = result
    except:
        content.loc[result.index, 'GPT_Comment'] = result.values.tolist()
        
    gpt_pos = 1*content.loc[result.index,].GPT_Comment.str.upper().str.contains('YES')\
            - 1*content.loc[result.index,].GPT_Comment.str.upper().str.contains('NO')
    gpt_pos.name = "GPT_Signal"
    content.loc[result.index,'GPT_Signal'] = gpt_pos
    try:
        ssql = sqlite3.connect('/home/quantcargillmetals/FlashNewsSQL.db')
        content.loc[result.index,].to_sql('MySteel_FlashNews', con = ssql, 
                                          if_exists='append', index=False, method = None)
        ssql.close()
    except:
        time.sleep(2)
        ssql = sqlite3.connect('/home/quantcargillmetals/FlashNewsSQL.db')
        content.loc[result.index,].to_sql('MySteel_FlashNews', con = ssql, 
                                          if_exists='append', index=False, method = None)
        ssql.close()
    time.sleep(3)
    # Print & Save
    if i % 100 == 0:
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        print("Current index: ", i, ".", current_time)
        #pd.DataFrame(results).to_csv("test_jin10.csv")
        
if (content.shape[0]%10) != 0:
    print('processing recent info')
    prompt_text = []
    time_text = []
    for nn in range(content.shape[0] - content.shape[0]%10, content.shape[0]):
        news_text = content.flashNewsContent.iloc[nn]
        time_text.append(nn)
        try:
            prompt_text.append(instruction_text + '\n\n' + news_text + '。')
        except:
            if np.isnan(news_text):
                prompt_text.append('Pls return Not News')
            else:
                prompt_text.append('Pls return Error')
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
    result.index = content.iloc[time_text,].index
    try:
        content.loc[result.index, 'GPT_Comment'] = result
    except:
        content.loc[result.index, 'GPT_Comment'] = result.values.tolist()
        
    gpt_pos = 1*content.loc[result.index,].GPT_Comment.str.upper().str.contains('YES')\
            - 1*content.loc[result.index,].GPT_Comment.str.upper().str.contains('NO')
    gpt_pos.name = "GPT_Signal"
    content.loc[result.index,'GPT_Signal'] = gpt_pos
    try:
        ssql = sqlite3.connect('/home/quantcargillmetals/FlashNewsSQL.db')
        content.loc[result.index,].to_sql('MySteel_FlashNews', con = ssql, 
                                          if_exists='append', index=False, method = None)
        ssql.close()
    except:
        time.sleep(2)
        ssql = sqlite3.connect('/home/quantcargillmetals/FlashNewsSQL.db')
        content.loc[result.index,].to_sql('MySteel_FlashNews', con = ssql, 
                                          if_exists='append', index=False, method = None)
        ssql.close()
    time.sleep(3)
with open('/home/quantcargillmetals/mysteel_gpt_log','a+') as log_f:
    print(f"*** [{dt_.datetime.now(tz)}] mysteel gpt comment added ***")
    log_f.writelines([f"*** [{dt_.datetime.now(tz)}] mysteel gpt comment added ***\n"])
