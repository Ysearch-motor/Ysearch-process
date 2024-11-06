import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd

def crawl(url, max_pages=5):
    pages_to_visit = [url]
    visited_pages = set()
    data = []

    while pages_to_visit and len(visited_pages) < max_pages:
        current_url = pages_to_visit.pop(0)
        if current_url in visited_pages:
            continue

        try:
            response = requests.get(current_url)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Failed to retrieve {current_url}: {e}")
            continue

        visited_pages.add(current_url)
        soup = BeautifulSoup(response.text, 'html.parser')

        for link in soup.find_all('a', href=True):
            href = link['href']
            full_url = urljoin(current_url, href)
            if full_url not in visited_pages and full_url not in pages_to_visit:
                pages_to_visit.append(full_url)
            if full_url not in data:
                if full_url.startswith('http'):
                    data.append(full_url)
                else:
                    continue
            else:
                continue

        print(f"Visited {current_url}")

    return data

if __name__ == "__main__":
    start_url = "https://www.lemonde.fr/"  # Replace with the target URL
    crawled_data = crawl(start_url)
    print("Crawled data:", crawled_data)
    df = pd.DataFrame(crawled_data, columns=['URL'])
    df.to_csv('crawled_data.csv', index=False)
    for i in range(0,100,1):
        df = pd.DataFrame(crawl(crawled_data[i]))
        df.to_csv('crawled_data.csv',mode='a', index=False)