import requests

data = {
    "original_url": "https://www.google.com",
    "expiry_days": 7
}

response = requests.post("http://127.0.0.1:8000/shorten", json=data)
print(response.status_code)
print(response.json())
