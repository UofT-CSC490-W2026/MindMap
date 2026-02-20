import requests
import feedparser
import os

# Search query
url = "http://export.arxiv.org/api/query"
params = {
    "search_query": "all:transformer",
    "start": 0,
    "max_results": 1
}

response = requests.get(url, params=params)
feed = feedparser.parse(response.text)

# Create folder
os.makedirs("papers", exist_ok=True)

for entry in feed.entries:
    title = entry.title.replace("\n", " ").strip()
    pdf_link = entry.id.replace("abs", "pdf") + ".pdf"

    print(f"Downloading: {title}")
    
    pdf_response = requests.get(pdf_link)
    filename = f"papers/{entry.id.split('/')[-1]}.pdf"
    
    with open(filename, "wb") as f:
        f.write(pdf_response.content)

    print(f"Saved as {filename}")
