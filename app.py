
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
            
    # 2. Filters & Actions
    st.subheader("Filters & Actions")
    
    # late analysis button
    if "final_status" not in df.columns or df['final_status'].isnull().all():
         col_act1, col_act2 = st.columns([1, 3])
         with col_act1:
             if st.button("Analyze URLs Now (SEO)"):
                 update_analysis(df['sitemap_url'].tolist()) # Changed run_late_analysis to update_analysis
    
    col_f1, col_f2, col_f3 = st.columns(3)
    
    # Always show filter, but options depend on data
    options = ["All"]
    if "final_status" in df.columns:
        options += ["200 OK", "Errors (4xx, 5xx)", "Redirects (3xx)", "Noindex", "Non-Canonical"]
        
    with col_f1:
        filter_status = st.selectbox("Filter Status", options)
    
    with col_f2:
        filter_text = st.text_input("Search URL", "")
    
    # Apply Filters
    if filter_status == "200 OK":
        df = df[df['final_status'] == 200]
    elif filter_status == "Errors (4xx, 5xx)":
        df = df[df['final_status'] >= 400]
    elif filter_status == "Redirects (3xx)":
         df = df[df['final_status'].between(300, 399)]
    elif filter_status == "Noindex":
         if "noindex" in df.columns:
             df = df[df['noindex'] == True]
    elif filter_status == "Non-Canonical":
         if "canonical_match" in df.columns:
             df = df[df['canonical_match'] == False]
         
    if filter_text:
        df = df[df['sitemap_url'].str.contains(filter_text, case=False, na=False)]

    filtered_count = len(df)
    
    # 3. Detailed Stats (Above Table)
    st.markdown("### Statistics")
    m1, m2, m3, m4, m5 = st.columns(5)
    
    m1.metric("Total URLs", len(st.session_state.df_results)) # Use original DF for totals
    
    if "final_status" in st.session_state.df_results.columns:
        orig_df = st.session_state.df_results
        ok_count = len(orig_df[orig_df['final_status'] == 200])
        m2.metric("200 OK", ok_count)
        
        if "noindex" in orig_df.columns:
            noindex_cnt = orig_df['noindex'].sum()
            m3.metric("Noindex", int(noindex_cnt))
        else:
            m3.metric("Noindex", "N/A")
            
        if "canonical_match" in orig_df.columns:
            non_canon_cnt = len(orig_df[orig_df['canonical_match'] == False])
            m4.metric("Non-Canonical", int(non_canon_cnt))
        else:
            m4.metric("Non-Canonical", "N/A")
            
        err_cnt = len(orig_df[orig_df['final_status'] >= 400])
        m5.metric("Errors", err_cnt)


    st.caption(f"Showing {filtered_count} URLs in current view")
    
    # 4. Data Table with Icons
    
    # Prepare Display Columns
    display_df = df.copy()
    
    if "final_status" in display_df.columns:
        display_df['Status Icon'] = display_df['final_status'].apply(lambda x: True if x == 200 else False)
    
    if "canonical_match" in display_df.columns:
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
    
    if st.session_state.page_number >= total_pages:
        st.session_state.page_number = 0
        
    start_idx = st.session_state.page_number * ITEMS_PER_PAGE
    end_idx = start_idx + ITEMS_PER_PAGE
    
    # Selection
    selection = st.dataframe(
        display_df.iloc[start_idx:end_idx], 
        use_container_width=True,
        column_config=column_config,
        on_select="rerun", 
        selection_mode="multi-row" 
    )
    
    selected_indices = selection.selection.rows
    selected_urls = []
    if selected_indices:
        slice_df = display_df.iloc[start_idx:end_idx]
        selected_urls = slice_df.iloc[selected_indices]['sitemap_url'].tolist()
        
    if selected_urls:
         st.info(f"Selected {len(selected_urls)} URLs")
         if st.button("Re-Analyze Selected URLs"):
             update_analysis(selected_urls)

    # 5. Smart Pagination (Bottom, Right-Aligned)
    st.divider()
    
    # We want alignment to the RIGHT.
    # We allocate a large empty column first, then the controls.
    # Structure: [Spacer (6)] [Prev (1)] [Btns...] [Next (1)]
    
    current = st.session_state.page_number + 1
    
    def set_page(p):
        st.session_state.page_number = p - 1
        st.rerun()

    # Determine elements to show
    # We will build a list of elements to render, then put them in right-aligned columns
    
    if total_pages <= 7:
        # Simple list: Prev 1 2 3 ... Next
        # Num elements = 1 (prev) + total_pages + 1 (next)
        num_cols = 1 + total_pages + 1
        
        # Calculate spacer
        # We assume 12 grid units usually nearby, but st.columns takes ratios/widths.
        # Let's try [4, 1, 1, ... 1]
        
        cols_config = [6] + [1] * num_cols
        cols = st.columns(cols_config)
        
        # cols[0] is spacer
        with cols[1]:
            if st.button("◀", disabled=(current == 1), help="Previous"):
                 set_page(current - 1)
                 
        for i in range(1, total_pages + 1):
            with cols[1+i]:
                if st.button(str(i), key=f"p_{i}", type="primary" if i == current else "secondary"):
                    set_page(i)
                    
        with cols[-1]:
             if st.button("▶", disabled=(current == total_pages), help="Next"):
                set_page(current + 1)
                
    else:
        # Complex: Prev 1 2 3 ... Input ... N-2 N-1 N Next
        # Elements: Prev (1), 1 (1), 2 (1), 3 (1), Input (2), N-2 (1), N-1 (1), N (1), Next (1)
        # Total widths: 1+1+1+1+2+1+1+1+1 = 10 units?
        # Spacer needs to be large.
        
        cols = st.columns([5, 1, 1, 1, 1, 2, 1, 1, 1, 1])
        # cols[0] is spacer
        
        with cols[1]:
             if st.button("◀", disabled=(current == 1)): set_page(current - 1)
        
        with cols[2]: 
            if st.button("1", key="p_1", type="primary" if 1 == current else "secondary"): set_page(1)
        with cols[3]: 
            if st.button("2", key="p_2", type="primary" if 2 == current else "secondary"): set_page(2)
        with cols[4]: 
            if st.button("3", key="p_3", type="primary" if 3 == current else "secondary"): set_page(3)
            
        with cols[5]:
             page_in = st.number_input("Go to", min_value=1, max_value=total_pages, value=current, label_visibility="collapsed")
             if page_in != current:
                set_page(page_in)
        
        with cols[6]: 
            if st.button(str(total_pages-2), key=f"p_{total_pages-2}", type="primary" if total_pages-2 == current else "secondary"): set_page(total_pages-2)
        with cols[7]: 
            if st.button(str(total_pages-1), key=f"p_{total_pages-1}", type="primary" if total_pages-1 == current else "secondary"): set_page(total_pages-1)
        with cols[8]: 
            if st.button(str(total_pages), key=f"p_{total_pages}", type="primary" if total_pages == current else "secondary"): set_page(total_pages)
            
        with cols[9]:
             if st.button("▶", disabled=(current == total_pages)): set_page(current + 1)
            
    # 6. Downloads
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

