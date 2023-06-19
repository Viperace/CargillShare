import pandas as pd
import numpy as np
import sqlite3
import datetime as dt_

ssql = sqlite3.connect('/home/quantcargillmetals/FlashNewsSQL.db')
db_mysteel = pd.read_sql('SELECT * FROM MySteel_FlashNews', con = ssql)
ssql.close()
db_mysteel.datetime = pd.to_datetime(db_mysteel.datetime)
db_mysteel = db_mysteel.set_index('datetime')
db_mysteel.sort_index(inplace = True)

time_begin = dt_.datetime.now() - dt_.timedelta(days = 30)

gpt_signal_sum = pd.DataFrame()
for i in range(24):
    gpt_signal_ = db_mysteel.GPT_Signal.resample('1d', origin = dt_.datetime.now()\
                                               - dt_.timedelta(hours=i),
                                               label='right').sum()[time_begin:dt_.datetime.now()]
    gpt_signal_sum = pd.concat([gpt_signal_sum, gpt_signal_])
    
new_mdf = gpt_signal_sum.rename(columns={0:"mysteel_gpt_score"}).sort_index(ascending = False)
new_mdf.to_csv('/home/quantcargillmetals/mysteel_gpt_signal_live_update.csv')
new_mdf.to_html('/home/hayes_zhu/mysteel_gpt_signal_live_update.html')
