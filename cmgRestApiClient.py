import requests
import pandas as pd
from datetime import datetime
from datetime import timedelta
import pytz
from tgapikey import api_key
from pprint import pprint as pp

utc_now = pytz.utc.localize(datetime.utcnow())
n=24   
past_time = (utc_now - timedelta(hours=n))
ct = (utc_now).isoformat()
pt = (past_time).isoformat()
url = "https://{subdomain}.api.cmgecm.com/dlgw/api/v1/offerings/query"


headers = {
    'Authorization': f"Bearer {api_key}",
    'Content-Type': "application/json; charset=utf-8"
}


filters = {
    'includeTotals': True,
    'customFilters': {'rules': [
        {
          "criteria": "MODIFIED_AT",
          "operatorType": "BETWEEN",
          "values": [pt,ct],
        }
            ]},
    'orderField': 'modifiedAt',
    'orderDirection': 'DESC',
    'perPage': 100,
    'page': 1,
}

response = requests.post(url,headers=headers,json=filters).json()
offerings = response['data']
 

for i in range (1,response['pagination']['totalPages']+1):
    filters['page'] = response['pagination']['activePage']+1
    response = requests.post(url,headers=headers,json=filters).json()
    offerings.extend(response["data"])
    print(response['pagination'])
 

pp(response)
database = pd.DataFrame(offerings)
database.to_csv(f'last48Hours{datetime.today().strftime("%Y-%m-%d_%H:%M:%S")}.csv') 