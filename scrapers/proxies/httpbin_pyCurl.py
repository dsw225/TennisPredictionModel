import pycurl
from io import BytesIO

url = "https://www.example.com"

buffer = BytesIO()

c = pycurl.Curl()

c.setopt(c.URL, url)

c.setopt(c.WRITEDATA, buffer)

c.perform()

http_code = c.getinfo(pycurl.HTTP_CODE)
print(f"HTTP Response Code: {http_code}")

c.close()

body = buffer.getvalue().decode('utf-8')
print(body)
