import requests
import http.client
import json
from urllib3.util import SKIP_HEADER

base_url = "https://api.sofascore.com"

headers = {
    'User-Agent': SKIP_HEADER,
    'Accept-Encoding': 'identity',
    'Accept': None,
    'Sec-Fetch-Mode': 'cors'
}

response = requests.get(base_url + '/api/v1/event/12432672', headers=headers)

tempstr = str(response.headers)

print("Requests: \n", tempstr.replace(",", "\n"))
print()


payload = ''
headers = {}

conn = http.client.HTTPSConnection("api.sofascore.com")
conn.request("GET", '/api/v1/event/12432672', payload, headers)
res = conn.getresponse()
data = res.read()
json_data = json.loads(data.decode("utf-8"))


# print("HTTP Lib: ", json_data)
print("HTTPClient")
print(res.headers)