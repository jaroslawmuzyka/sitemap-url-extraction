
import streamlit as st
import pandas as pd
import asyncio
from sitemap_parser import parse_uploaded_file, parse_sitemap, fetch_sitemap_content, extract_urls_recursive
from seo_analyzer import analyze_urls

# Set page config
st.set_page_config(page_title="Sitemap URL Extractor", page_icon="🔗", layout="wide")

# Password Protection
if "APP_PASSWORD" in st.secrets:
    password = st.secrets["APP_PASSWORD"]
    if "password_correct" not in st.session_state:
        st.session_state.password_correct = False
    
    if not st.session_state.password_correct:
         st.markdown("## 🔒 Login Required")
         input_pass = st.text_input("Enter Password", type="password")
         if st.button("Login"):
             if input_pass == password:
                 st.session_state.password_correct = True
                 st.rerun()
             else:
                 st.error("Incorrect password")
         st.stop()
else:
    # Optional warning, can be removed if not needed contextually
    pass

# Initialize Session State
if "df_results" not in st.session_state:
    st.session_state.df_results = None
if "processing_done" not in st.session_state:
    st.session_state.processing_done = False
if "page_number" not in st.session_state:
    st.session_state.page_number = 0

ITEMS_PER_PAGE = 100

st.title("🔗 Sitemap URL Extractor & SEO Analyzer")

# Sidebar Configuration
st.sidebar.header("Configuration")
mode = st.sidebar.radio("Input Mode", ["Upload XML File", "Enter Sitemap URL"])
limit_urls = st.sidebar.number_input("Limit URLs", value=50000, step=1000)
do_seo = st.sidebar.checkbox("Perform SEO Analysis", value=False, help="Check status codes, canonicals, noindex. Can be slow.")

if st.sidebar.button("Clear Results"):
    st.session_state.df_results = None
    st.session_state.processing_done = False
    st.session_state.page_number = 0
    st.rerun()

# Logic to run processing
def run_processing(url_source, is_upload=False):
    st.session_state.processing_done = False
    st.session_state.df_results = None
    st.session_state.page_number = 0
    
    urls = []
    
    with st.spinner("Extracting URLs..."):
        if is_upload:
            all_urls = set()
            for f in url_source:
                # Reset file pointer if needed, but uploaded_file usually OK
                file_urls = parse_uploaded_file(f.read())
                all_urls.update(file_urls)
            urls = list(all_urls)
        else:
            # fetch content to validate
            content = fetch_sitemap_content(url_source)
            if not content:
                st.error("Failed to fetch sitemap.")
                return
            urls = extract_urls_recursive(url_source, max_urls=limit_urls)
    
    # Trim to limit
    urls = urls[:limit_urls]
    
    if not urls:
        st.warning("No URLs found.")
        return

    # Basic DF
    df = pd.DataFrame({'sitemap_url': urls})
    
    if do_seo:
        st.info(f"Starting SEO Analysis for {len(urls)} URLs... This may take time.")
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        async def run_analysis():
            results = await analyze_urls(urls, lambda p: progress_bar.progress(p))
            return results
            
        try:
            analyzed_data = asyncio.run(run_analysis())
            df = pd.DataFrame(analyzed_data)
            status_text.text("Analysis Complete!")
        except Exception as e:
            st.error(f"Error during analysis: {e}")
            
    st.session_state.df_results = df
    st.session_state.processing_done = True
    st.rerun()

# User Inputs
if mode == "Upload XML File":
    uploaded_files = st.file_uploader("Upload XML Sitemap(s)", accept_multiple_files=True, type=['xml', 'gz'])
    if st.button("Start Processing"):
        if uploaded_files:
            run_processing(uploaded_files, is_upload=True)
        else:
            st.warning("Please upload a file.")

elif mode == "Enter Sitemap URL":
    sitemap_url = st.text_input("Enter Sitemap URL (e.g., https://example.com/sitemap.xml)")
    if st.button("Start Processing"):
        if sitemap_url:
            run_processing(sitemap_url, is_upload=False)
        else:
            st.warning("Please enter a URL.")

# Display Results
if st.session_state.processing_done and st.session_state.df_results is not None:
    df = st.session_state.df_results
    
    st.divider()
    
    # 1. Dashboard Metrics
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total URLs", len(df))
    
    if "final_status" in df.columns:
        # Determine status counts
        # Handle NaN/None carefully
        statuses = df['final_status'].fillna(0).astype(int)
        
        ok_count = (statuses == 200).sum()
        redirect_count = statuses.isin([301, 302, 303, 307, 308]).sum()
        error_count = (statuses >= 400).sum()
        if 'fetch_error' in df.columns:
             error_count += df['fetch_error'].notnull().sum()
        
        m2.metric("200 OK", int(ok_count))
        m3.metric("Redirects", int(redirect_count))
        m4.metric("Errors", int(error_count))
        
        # Advanced Stats
        st.markdown("### SEO Details")
        s1, s2, s3 = st.columns(3)
        
        if 'noindex' in df.columns:
            noindex_count = df['noindex'].sum()
            s1.metric("Noindex", int(noindex_count))
            
        if 'canonical_match' in df.columns:
            # canonical_match might be boolean or NaN if not checked
            non_canonical_count = len(df[df['canonical_match'] == False])
            s2.metric("Non-Canonical", int(non_canonical_count))
            
    # 2. Results Table with Pagination
    st.subheader("Results")
    
    total_pages = max(1, (len(df) - 1) // ITEMS_PER_PAGE + 1)
    
    c_prev, c_page, c_next = st.columns([1, 2, 1])
    
    with c_prev:
        if st.button("Previous"):
            if st.session_state.page_number > 0:
                st.session_state.page_number -= 1
                st.rerun()
                
    with c_page:
        st.write(f"Page {st.session_state.page_number + 1} of {total_pages}")
        
    with c_next:
        if st.button("Next"):
            if st.session_state.page_number < total_pages - 1:
                st.session_state.page_number += 1
                st.rerun()
    
    # Display Slice
    start_idx = st.session_state.page_number * ITEMS_PER_PAGE
    end_idx = start_idx + ITEMS_PER_PAGE
    
    # Style the dataframe?
    st.dataframe(df.iloc[start_idx:end_idx], use_container_width=True)
    
    # 3. Download
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Download Full CSV Results",
        data=csv,
        file_name="sitemap_analysis.csv",
        mime="text/csv"
    )

# Sidebar Example
st.sidebar.markdown("---")
st.sidebar.markdown("### Test Data")
try:
    with open("example-sitemap.xml", "rb") as f:
        st.sidebar.download_button("Download Example Sitemap", f, "example-sitemap.xml")
except FileNotFoundError:
    st.sidebar.warning("Example file not found.")
