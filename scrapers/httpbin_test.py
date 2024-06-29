import requests
import http.client
import json
from urllib3.util import SKIP_HEADER

base_url = "https://httpbin.org"

headers = {
    'User-Agent': SKIP_HEADER,
    'Accept-Encoding': 'identity',
    'Accept': None,
    'Content-Length': '0'
}

response = requests.get(base_url + '/get', headers=headers)
response = response.json()

print("Requests: ", response)

http.client.HTTPSConnection("api.sofascore.com")

payload = ''
headers = {}

conn = http.client.HTTPSConnection("httpbin.org")
conn.request("GET", '/get', payload, headers)
res = conn.getresponse()
data = res.read()
json_data = json.loads(data.decode("utf-8"))

print("HTTP Lib: ", json_data)
print(res.version)