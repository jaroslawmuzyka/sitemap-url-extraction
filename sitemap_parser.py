import gzip
import io
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from curl_cffi import requests

def is_gzip(content):
    """Check if content is gzipped based on magic numbers."""
    return content[:2] == b'\x1f\x8b'

def fetch_sitemap_content(url):
    """Fetch sitemap content using curl_cffi to impersonate Chrome."""
    try:
        # impersonate="chrome" sends TLS fingerprints matching real Chrome
        response = requests.get(url, impersonate="chrome", timeout=10)
        response.raise_for_status()
        
        content = response.content
        if is_gzip(content) or url.endswith('.gz'):
            try:
                content = gzip.decompress(content)
            except OSError:
                # Fallback if it looked like gzip but wasn't
                pass
        return content, None  # content, error_msg
    except Exception as e:
        return None, f"Error fetching {url}: {e}"

def parse_sitemap(content):
    """Parse sitemap content and return lists of URLs and child sitemaps."""
    if not content:
        return [], []
        
    soup = BeautifulSoup(content, 'lxml-xml')
    
    # Extract URLs
    urls = [loc.text.strip() for loc in soup.find_all('loc')]
    
    sitemap_index_urls = []
    final_urls = []
    
    for tag in soup.find_all('loc'):
        parent = tag.parent
        if parent.name == 'sitemap':
            sitemap_index_urls.append(tag.text.strip())
        elif parent.name == 'url':
            final_urls.append(tag.text.strip())
            
    return final_urls, sitemap_index_urls

def extract_urls_recursive(url, max_urls=1000000, should_stop=None):
    """Recursively extract URLs from a sitemap or sitemap index."""
    all_urls = [] # List of dicts {'sitemap_url': url, 'source_sitemap': source}
    seen_urls = set()
    
    to_process = [url]
    processed_sitemaps = [] # List for order
    processed_sitemaps_set = set() # Set for check
    
    errors = []
    
    while to_process and len(all_urls) < max_urls:
        if should_stop and should_stop():
            break
            
        current_sitemap = to_process.pop(0)
        
        if current_sitemap in processed_sitemaps_set:
            continue
            
        print(f"Processing: {current_sitemap}")
        processed_sitemaps.append(current_sitemap)
        processed_sitemaps_set.add(current_sitemap)
        
        content, error_msg = fetch_sitemap_content(current_sitemap)
        if error_msg:
            errors.append(error_msg)
            continue
            
        urls, child_sitemaps = parse_sitemap(content)
        
        # Add found URLs
        for u in urls:
            if u not in seen_urls:
                seen_urls.add(u)
                all_urls.append({'sitemap_url': u, 'source_sitemap': current_sitemap})
                if len(all_urls) >= max_urls:
                    break
        
        # Add child sitemaps to queue (ordered)
        for child in child_sitemaps:
            if child not in processed_sitemaps_set:
                to_process.append(child)
                
    return all_urls, processed_sitemaps, errors

def parse_uploaded_file(file_content, filename):
    """Parse a single uploaded file (bytes)."""
    urls, _ = parse_sitemap(file_content)
    return [{'sitemap_url': u, 'source_sitemap': filename} for u in urls]
