
import streamlit as st
import pandas as pd
import asyncio
import io
from sitemap_parser import parse_uploaded_file, parse_sitemap, fetch_sitemap_content, extract_urls_recursive
from seo_analyzer import analyze_urls
from curl_cffi.requests import RequestsError # For error handling context

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
    # Optional warning
    pass

# Initialize Session State
if "df_results" not in st.session_state:
    st.session_state.df_results = None
if "processed_sitemaps" not in st.session_state:
    st.session_state.processed_sitemaps = []
if "processing_done" not in st.session_state:
    st.session_state.processing_done = False
if "page_number" not in st.session_state:
    st.session_state.page_number = 0

ITEMS_PER_PAGE = 100

st.title("🔗 Sitemap URL Extractor & SEO Analyzer")

# Sidebar
st.sidebar.header("Configuration")
mode = st.sidebar.radio("Input Mode", ["Upload XML File", "Enter Sitemap URL"])
limit_urls = st.sidebar.number_input("Limit URLs", value=50000, step=1000)
do_seo = st.sidebar.checkbox("Perform SEO Analysis", value=False, help="Check status codes, canonicals, noindex.")

if st.sidebar.button("Clear Results"):
    st.session_state.df_results = None
    st.session_state.processed_sitemaps = []
    st.session_state.processing_done = False
    st.session_state.page_number = 0
    st.rerun()

# Async Analysis Runner
async def run_seo_analysis_async(urls, status_container):
    progress_bar = status_container.progress(0)
    results = await analyze_urls(urls, lambda p: progress_bar.progress(p))
    return results

# Processing Logic
def run_processing(url_source, is_upload=False):
    st.session_state.processing_done = False
    st.session_state.df_results = None
    st.session_state.processed_sitemaps = []
    st.session_state.page_number = 0
    
    urls = []
    found_sitemaps = []
    
    with st.spinner("Extracting URLs..."):
        if is_upload:
            all_urls = set()
            for f in url_source:
                f.seek(0)
                file_urls = parse_uploaded_file(f.read())
                all_urls.update(file_urls)
            urls = list(all_urls)
            found_sitemaps = [f.name for f in url_source]
        else:
            # fetch content to validate
            urls, found_sitemaps = extract_urls_recursive(url_source, max_urls=limit_urls)
    
    urls = urls[:limit_urls]
    
    if not urls:
        st.warning("No URLs found.")
        return

    df = pd.DataFrame({'sitemap_url': urls})
    
    if do_seo:
        st.info(f"Starting SEO Analysis for {len(urls)} URLs...")
        placeholder = st.empty()
        try:
            analyzed_data = asyncio.run(run_seo_analysis_async(urls, placeholder))
            df = pd.DataFrame(analyzed_data)
            placeholder.text("Analysis Complete!")
        except Exception as e:
            st.error(f"Error during analysis: {e}")
            
    st.session_state.df_results = df
    st.session_state.processed_sitemaps = found_sitemaps
    st.session_state.processing_done = True
    st.rerun()

# Helper for Late/Re-Analysis
def update_analysis(target_urls):
    if not target_urls:
         st.warning("No URLs selected to analyze.")
         return
         
    st.info(f"Analyzing {len(target_urls)} URLs...")
    placeholder = st.empty()
    try:
        new_results = asyncio.run(run_seo_analysis_async(target_urls, placeholder))
        new_df = pd.DataFrame(new_results)
        
        # Merge back into main DF
        # We need to align by sitemap_url
        current_df = st.session_state.df_results
        
        # Set index to url for update
        current_df.set_index('sitemap_url', inplace=True)
        new_df.set_index('sitemap_url', inplace=True)
        
        # Update columns
        current_df.update(new_df)
        
        # Reset index
        current_df.reset_index(inplace=True)
        st.session_state.df_results = current_df
        
        placeholder.success("Re-analysis Complete!")
        st.rerun()
    except Exception as e:
        st.error(f"Error: {e}")

# INPUT SECTION
if mode == "Upload XML File":
    uploaded_files = st.file_uploader("Upload XML", accept_multiple_files=True, type=['xml', 'gz'])
    if st.button("Start Processing"):
        if uploaded_files:
             run_processing(uploaded_files, is_upload=True)
        else:
             st.warning("Upload a file.")
elif mode == "Enter Sitemap URL":
    sitemap_url = st.text_input("Sitemap URL", "https://example.com/sitemap.xml")
    if st.button("Start Processing"):
        if sitemap_url:
             run_processing(sitemap_url, is_upload=False)
        else:
             st.warning("Enter a URL.")

# RESULTS SECTION
if st.session_state.processing_done and st.session_state.df_results is not None:
    df = st.session_state.df_results.copy()
    
    st.divider()
    
    # 1. Processed Sitemaps Info
    if st.session_state.processed_sitemaps:
        with st.expander(f"📦 Found {len(st.session_state.processed_sitemaps)} Sitemaps in Index"):
            st.write(st.session_state.processed_sitemaps)
            
    # 2. Filters
    st.subheader("Filters & Actions")
    col_f1, col_f2, col_f3 = st.columns(3)
    
    filter_status = "All"
    if "final_status" in df.columns:
        options = ["All", "200 OK", "Errors (4xx, 5xx)", "Redirects (3xx)"]
        filter_status = col_f1.selectbox("Filter Status", options)
    
    filter_text = col_f2.text_input("Search URL", "")
    
    # Apply Filters
    if filter_status == "200 OK":
        df = df[df['final_status'] == 200]
    elif filter_status == "Errors (4xx, 5xx)":
        df = df[df['final_status'] >= 400]
    elif filter_status == "Redirects (3xx)":
         df = df[df['final_status'].between(300, 399)]
         
    if filter_text:
        df = df[df['sitemap_url'].str.contains(filter_text, case=False, na=False)]

    filtered_count = len(df)
    st.caption(f"Showing {filtered_count} URLs")
    
    # 3. Data Table with Icons
    
    # Prepare Display Columns
    # Map logic to boolean for icons
    display_df = df.copy()
    
    if "final_status" in display_df.columns:
        display_df['Status Icon'] = display_df['final_status'].apply(lambda x: True if x == 200 else False)
    
    if "canonical_match" in display_df.columns:
        # Ensure boolean
        display_df['canonical_match'] = display_df['canonical_match'].fillna(False).astype(bool)

    # Column Configuration
    column_config = {
        "sitemap_url": st.column_config.LinkColumn("URL"),
        "Status Icon": st.column_config.CheckboxColumn("Status OK", width="small"),
        "final_status": st.column_config.NumberColumn("Code", format="%d"),
        "canonical_match": st.column_config.CheckboxColumn("Canonical Match", width="small"),
        "noindex": st.column_config.CheckboxColumn("Noindex", width="small"),
    }
    
    # Pagination Logic
    total_pages = max(1, (len(display_df) - 1) // ITEMS_PER_PAGE + 1)
    
    # Ensure page valid
    if st.session_state.page_number >= total_pages:
        st.session_state.page_number = 0
        
    start_idx = st.session_state.page_number * ITEMS_PER_PAGE
    end_idx = start_idx + ITEMS_PER_PAGE
    
    # Selection in Dataframe
    selection = st.dataframe(
        display_df.iloc[start_idx:end_idx], 
        use_container_width=True,
        column_config=column_config,
        on_select="rerun", # Enable selection
        selection_mode="multi-row" 
    )
    
    selected_indices = selection.selection.rows
    # These indices are relative to the SLICE (0 to 100). Need to map to DF.
    # Actually, st.dataframe returns row indices of the original dataframe passed to it if index is preserved?
    # No, it returns integer usage indices of the displayed data (0-N).
    # We must start_idx + selected_row_index to get index in display_df.
    
    selected_urls = []
    if selected_indices:
        # Map visual index to real DF index
        # display_df.iloc[...] is a slice. The row ID from on_select refers to the position in that slice.
        slice_df = display_df.iloc[start_idx:end_idx]
        selected_urls = slice_df.iloc[selected_indices]['sitemap_url'].tolist()
        
    if selected_urls:
        st.error(f"Selected {len(selected_urls)} URLs for Re-Analysis")
        if st.button("Re-Analyze Selected URLs"):
            update_analysis(selected_urls)

    # 4. Pagination Controls (Bottom)
    c_prev, c_input, c_next = st.columns([1, 2, 1])
    
    with c_prev:
        if st.button("Previous Page", disabled=(st.session_state.page_number == 0)):
            st.session_state.page_number -= 1
            st.rerun()
            
    with c_input:
        new_page = st.number_input(
            "Page Info", 
            min_value=1, max_value=total_pages, 
            value=st.session_state.page_number + 1,
            label_visibility="collapsed"
        )
        if new_page - 1 != st.session_state.page_number:
            st.session_state.page_number = new_page - 1
            st.rerun()
            
    with c_next:
        if st.button("Next Page", disabled=(st.session_state.page_number >= total_pages - 1)):
            st.session_state.page_number += 1
            st.rerun()
            
    # 5. Downloads
    st.divider()
    d1, d2 = st.columns(2)
    with d1:
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button("Download CSV", csv, "sitemap_analysis.csv", "text/csv")
    with d2:
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
        st.download_button("Download Excel", buffer.getvalue(), "sitemap_analysis.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
