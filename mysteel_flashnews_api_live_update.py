import pandas as pd
import time
import numpy as np
import requests
import hashlib
import sqlite3
import datetime
import pytz
tz = pytz.timezone('Asia/Hong_Kong')

TimeStamp = time.time()*1000
DF = pd.DataFrame([])
version="1.0.0"
timestamp=str(int(time.time()*1000))
appKey="19Y9BFUBJJI4NRZ0WLV6MXZY986A7IEK"
appSecret="D592000A8CA04E37950455437EF63D1C"

data={
  "orderId":"20230425164080",
  "productId":"k94",
  "pageSize":100,
  "pageNum":1,
  "startTime":TimeStamp-24*3600*(1)*1000,
  "endTime": TimeStamp
}

a = datetime.datetime.fromtimestamp(data["startTime"]/1000, tz =tz)

b = datetime.datetime.fromtimestamp(data["endTime"]/1000, tz =tz)

print(f"*** [{datetime.datetime.now(tz)}] deriving mysteel falshnews data ({a.ctime()} to {b.ctime()}) ***")

#生成签名
path = "/cmsArticleDock/searchInformationFlashNews";
sign_str = "path" + path + "timestamp" + timestamp +"version" +version + appSecret;
sign=hashlib.md5(sign_str.encode("utf-8")).hexdigest().upper()


#发送请求
url="http://openapi.mysteel.com/cmsArticleDock/searchInformationFlashNews"
headers={
  "Content-Type":"application/json",
  "version":version,
  "timestamp":timestamp,
  "appKey":appKey,
  "sign":sign
}
response=requests.post(url,headers=headers,json=data)

#处理响应
if response.status_code==200:
    result=response.json()
    try:
        df = pd.DataFrame(result)
        DF = pd.concat([DF, df])
    except:
        s = time.strftime("%Y-%m-%d", time.localtime(data['startTime']/1000))
        print(f"No data for {s}")
else:
    print(response)
try:
    for i in range(df.totalPageNum.unique()[0]-1):
        data={
              "orderId":"20230425164080",
              "productId":"k94",
              "pageSize":100,
              "pageNum":i+2,
              "startTime":TimeStamp-24*3600*(1)*1000,
              "endTime": TimeStamp}
        response=requests.post(url,headers=headers,json=data)
        if response.status_code==200:
            result=response.json()
            df = pd.DataFrame(result)
            DF = pd.concat([DF, df])
        else:
            print(response)
except:
        s = time.strftime("%Y-%m-%d", time.localtime(data['startTime']/1000))
        print(f"No data for {s}")

s = time.strftime("%Y-%m-%d", time.localtime(data['startTime']/1000))
e = time.strftime("%Y-%m-%d", time.localtime(data['endTime']/1000))
new_df = pd.DataFrame(DF.reset_index(drop = True).datas.tolist())
new_df.rename(columns = {"flashNewsPublishTime":"datetime"}, inplace = True)
new_df = new_df[["datetime", "flashNewsCategoryName", "flashNewsId",
                 "flashNewsContent", "flashNewsColumnName"]]

#with open('/home/quantcargillmetals/mysteel_api_log','a+') as log_f:
# print(f"*** [{datetime.datetime.now(tz)}] write to mysteel flashnews db ***")
# log_f.writelines([f"*** [{datetime.datetime.now(tz)}] write to mysteel flashnews db ***\n"])
#ssql = sqlite3.connect('/home/quantcargillmetals/FlashNewsSQL.db')

db_path = 'C:/Users/x994664/OneDrive - Cargill Inc/Documents/FlashNewsSQL.db'
ssql = sqlite3.connect(db_path)
new_df['datetime'] = pd.to_datetime(new_df['datetime'])
new_df.to_sql('MySteel_FlashNews',con = ssql,if_exists='append', index=False, method = None)
ssql.close()
