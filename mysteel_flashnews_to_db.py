import pandas as pd
import sqlite3 as sqlconn
conn = sqlconn.connect(database =r'C:\Users\h870692\OneDrive - Cargill Inc\FlashNewsSQL.db')
conn.text_factory = sqlconn.OptimizedUnicode

GPT_Signal = pd.read_csv(r"\\mscnshan022.ap.corp.cargill.com\data\Structure Team\SIGNAL OUTPUT\MySteel_FlashNews\MySteel_FlashNews_With_GPT_Comment.csv",
                     index_col = 'datetime')
GPT_Signal = GPT_Signal[~GPT_Signal.flashNewsContent.str.contains("最新价格")]
GPT_Pos = 1*GPT_Signal.GPT_Comment.str.upper().str.contains('YES') - 1*GPT_Signal.GPT_Comment.str.upper().str.contains('NO')
GPT_Pos.name = "GPT_Signal"
GPT_Signal['GPT_Signal'] = GPT_Pos
GPT_Signal.index = pd.to_datetime(pd.to_datetime(GPT_Signal.index).strftime("%Y-%m-%d %H:%M:00"))

GPT_Signal.reset_index().drop(columns="Unnamed: 0").to_sql('MySteel_FlashNews',
                                                           con = conn, if_exists='append', index=False, method = None)