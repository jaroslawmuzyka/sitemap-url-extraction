import requests
import gzip
import io
from bs4 import BeautifulSoup
from urllib.parse import urlparse

def is_gzip(content):
    """Check if content is gzipped based on magic numbers."""
    return content[:2] == b'\x1f\x8b'

def fetch_sitemap_content(url):
    """Fetch sitemap content, handling GZIP automatically."""
    headers = {'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        content = response.content
        if is_gzip(content) or url.endswith('.gz'):
            try:
                content = gzip.decompress(content)
            except OSError:
                # Fallback if it looked like gzip but wasn't
                pass
        return content
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

def parse_sitemap(content):
    """Parse sitemap content and return lists of URLs and child sitemaps."""
    if not content:
        return [], []
        
    soup = BeautifulSoup(content, 'lxml-xml')
    
    # Extract URLs
    urls = [loc.text.strip() for loc in soup.find_all('loc')]
    
    # Identify if it's a sitemap index by checking for <sitemap> tags
    # Sitemaps in an index are also in <loc> tags, but under <sitemap> parent
    # Standard sitemaps have <loc> under <url> parent
    
    sitemap_index_urls = []
    final_urls = []
    
    for tag in soup.find_all('loc'):
        parent = tag.parent
        if parent.name == 'sitemap':
            sitemap_index_urls.append(tag.text.strip())
        elif parent.name == 'url':
            final_urls.append(tag.text.strip())
            
    # If soup.find_all('sitemap') found nothing, but we have URLs, it's a regular sitemap
    # If we found <sitemap> tags, those specific locs are indices
    
    return final_urls, sitemap_index_urls

def extract_urls_recursive(url, max_urls=50000):
    """Recursively extract URLs from a sitemap or sitemap index."""
    all_urls = set()
    to_process = [url]
    processed_sitemaps = set()
    
    while to_process and len(all_urls) < max_urls:
        current_sitemap = to_process.pop(0)
        if current_sitemap in processed_sitemaps:
            continue
            
        print(f"Processing: {current_sitemap}")
        processed_sitemaps.add(current_sitemap)
        
        content = fetch_sitemap_content(current_sitemap)
        if not content:
            continue
            
        urls, child_sitemaps = parse_sitemap(content)
        
        # Add found URLs
        for u in urls:
            all_urls.add(u)
            if len(all_urls) >= max_urls:
                break
        
        # Add child sitemaps to queue
        for child in child_sitemaps:
            if child not in processed_sitemaps:
                to_process.append(child)
                
    return list(all_urls)

def parse_uploaded_file(file_content):
    """Parse a single uploaded file (bytes)."""
    urls, _ = parse_sitemap(file_content)
    return urls
