import requests

url = "https://serp.acen.http.321174.com/request_testasdadsa"

payload = {
    "engine": "bing",
    "q": "&**^^",
    "json": "1",
    "isjson": "1"
}

headers = {
    "Authorization": "Bearer abe7dd754215085fbe50f47b4ff1fcaf",
    "Content-Type": "application/x-www-form-urlencoded",
}

resp = requests.post(url, data=payload, headers=headers, timeout=30)
print(resp.text)
