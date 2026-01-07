import requests
from urllib.parse import urlencode
import json

API_URL = "https://api.brightdata.com/request"
#API_TOKEN = "178308b398771671aca4685bff17b69b86ce1549fedc0690683374d030b86e86"
API_TOKEN = "3c8389ed95f65afebdab9b371adf84071304e247d1636e0f0ff2ac0a3f03c2f8" # <-- Replace with your real Bright Data API token
ZONE = "serp_api1"  # <-- Replace with your actual SERP API zone name


def google_search(query):
    google_params = {
        "q": query,
        "brd_json": 1,  # Instructs Bright Data to parse the SERP into JSON
    }

    google_url = "https://www.google.com/search?" + urlencode(google_params)
    print(google_url)
    payload = {
        "zone": ZONE,
        "url": google_url,
        "format": "json",
    }

    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
    }

    resp = requests.post(API_URL, json=payload, headers=headers, timeout=60)
    resp.raise_for_status()  # raises an error for bad HTTP responses
    return resp.json()


if __name__ == "__main__":
    data = google_search("pizza")
    with open("serp_results.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("Results saved to serp_results.json")