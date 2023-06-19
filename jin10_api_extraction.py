import pandas as pd
import numpy as np
import re as re
import requests
import os
import time
from bs4 import BeautifulSoup
import datetime
import matplotlib.pyplot as plt
import hashlib
#from openpyxl import load_workbook
import json

from datetime import datetime
import os

nn = os.environ['var']

DF_Jin10 = pd.DataFrame()

for n in range(1,nn):
    headers = { "x-sec":"Gsqp7FRhde25eEmWks6T6y3U5mWT96NVf9"}
    body = {'limit': 100, 'page':n}
    print(n)
    url = "http://47.114.195.84:1425/qihuo-test"
    x = req.post(url, headers = headers, json = body)
    try:
        if x.status_code == 200:
            result = x.json()
            res = pd.DataFrame(pd.DataFrame(result).data.values.tolist()).set_index('time')
            DF_Jin10 = pd.concat([DF_Jin10, res])
        else:
            print("Request Failed")
        time.sleep(5)
    except:
        time.sleep(5)
        continue
DF_Jin10.to_csv("Jin10.csv")