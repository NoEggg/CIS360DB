import re
import streamlit as st
from database import run_query, run_write

st.set_page_config(
    page_title="Data Fusion KMS",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="expanded",
)

PAGES = ["Dashboard", "Papers", "Methods", "Datasets", "Contributors", "Search"]
page = st.sidebar.selectbox("Navigate", PAGES)


STOP_WORDS = {
    "a","an","the","is","are","was","were","in","on","at","to","for","of",
    "and","or","not","do","has","have","will","can","it","we","you",
    "show","find","get","list","give","used","using","about",
    # ... full list in source file
}

def build_regex(text):
    """Convert a query string to a case-insensitive Neo4j regex pattern."""
    words = text.split()
    keywords = [w.strip(".,!?;:\"'") for w in words
                if w.lower().strip(".,!?;:\"'") not in STOP_WORDS]
    if not keywords:
        keywords = words
    # Short queries → literal match
    if len(keywords) <= 2 and len(words) <= 2:
        return re.escape(text)
    # Long / natural-language → match any keyword
    return "|".join(re.escape(k) for k in keywords if k)


def metric_row(papers, methods, datasets, contributors):
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("📄 Papers",       papers)
    c2.metric("⚙️ Methods",      methods)
    c3.metric("🗄️ Datasets",    datasets)
    c4.metric("👥 Contributors", contributors)


def show_table(rows, columns=None):
    """Display a list of dicts as a styled dataframe."""
    if not rows:
        st.info("No records found.")
        return
    import pandas as pd
    df = pd.DataFrame(rows)
    if columns:
        df = df[[c for c in columns if c in df.columns]]
    st.dataframe(df, use_container_width=True, hide_index=True)


def paginate(items, per_page=25, key="page"):
    """Show a page number input and return the current page slice."""
    total = len(items)
    if total == 0:
        return []
    total_pages = max(1, (total + per_page - 1) // per_page)
    if total_pages > 1:
        page_num = st.number_input(
            f"Page (1–{total_pages})", min_value=1,
            max_value=total_pages, value=1, step=1, key=key,
        )
    else:
        page_num = 1
    start = (page_num - 1) * per_page
    st.caption(f"Showing {start + 1}–{min(start + per_page, total)} of {total}")
    return items[start:start + per_page]

if page == "Dashboard":
    st.title("🔬 Data Fusion KMS — Dashboard")

    counts = run_query("""
        RETURN
          count { MATCH (p:Paper)       RETURN p } AS papers,
          count { MATCH (m:Method)      RETURN m } AS methods,
          count { MATCH (d:Dataset)     RETURN d } AS datasets,
          count { MATCH (c:Contributor) RETURN c } AS contributors
    """)
    c = counts[0]
    metric_row(c["papers"], c["methods"], c["datasets"], c["contributors"]) 
    top_fields = run_query("""
        MATCH (p:Paper)
        WHERE p.field_of_study IS NOT NULL AND p.field_of_study <> ''
        RETURN p.field_of_study AS field, count(p) AS cnt
        ORDER BY cnt DESC LIMIT 10
    """)
elif page == "Papers":
    st.title("📄 Papers")

    col1, col2 = st.columns([3, 1])
    with col1:
        q = st.text_input("Search (title, author, DOI, abstract, keywords)")
    with col2:
        fields_raw = run_query("""
            MATCH (p:Paper)
            WHERE p.field_of_study IS NOT NULL AND p.field_of_study <> ''
            RETURN DISTINCT p.field_of_study AS f ORDER BY f
        """)
        field_opts = ["All Fields"] + [r["f"] for r in fields_raw]
        field_sel = st.selectbox("Field of Study", field_opts)
        filters = []
        params  = {}
        if q:
            rgx = build_regex(q)
            filters.append(
                "(p.title =~ $rgx OR p.author =~ $rgx OR p.doi =~ $rgx "
                " OR p.abstract =~ $rgx OR p.keywords =~ $rgx)"
            )
            params["rgx"] = f"(?i).*({rgx}).*"
        if field_sel != "All Fields":
            filters.append("p.field_of_study = $field")
            params["field"] = field_sel

        where = ("WHERE " + " AND ".join(filters)) if filters else ""

        rows = run_query(f"""
            MATCH (c:Contributor)-[:CONTRIBUTED]->(p:Paper)
            {where}
            RETURN elementId(p) AS eid, p.title AS title, p.author AS author,
                    p.doi AS doi, p.field_of_study AS field_of_study,
                    c.name AS contributor
            ORDER BY id(p) DESC
        """, params)

        if rows:
            options = {f"{r['title'] or r['doi'] or 'Untitled'} [{r['eid'][-6:]}]": r["eid"]
                for r in rows[:200]}
            selected_label = st.selectbox("Select a paper", list(options.keys()))
            eid = options[selected_label]

            detail = run_query(
                "MATCH (c:Contributor)-[:CONTRIBUTED]->(p:Paper) WHERE elementId(p)=$eid "
                "RETURN p, c.name AS contributor_name",
                {"eid": eid}
            )

        with st.expander("✏️ Edit this Paper"):
            with st.form("edit_paper_form"):
                title = st.text_input("Title", value=p.get('title') or "")
                # ... more fields ...
                submitted = st.form_submit_button("💾 Update Paper")
                if submitted:
                    run_write("""
                        MATCH (p:Paper) WHERE elementId(p) = $eid
                        SET p.title = $title, p.author = $author, ...
                    """, {"eid": eid, "title": title or None, ...})
                    st.success("Paper updated!")
                    st.rerun()

                if st.button("🗑️ Delete this Paper"):
                    run_write(
                        "MATCH (p:Paper) WHERE elementId(p)=$eid DETACH DELETE p",
                        {"eid": eid}
                    )
                    st.success("Paper deleted.")
                    st.rerun()

        st.subheader("➕ Add a New Paper")
        with st.form("add_paper_form"):
            doi   = st.text_input("DOI")
            title = st.text_input("Title")
            # ... more fields ...
            if st.form_submit_button("➕ Add Paper"):
                run_write("""
                    CREATE (p:Paper {doi:$doi, title:$title, ...})
                """, {"doi": doi or None, "title": title or None, ...})
                st.success(f"Paper '{title}' added!")
                st.rerun()
elif page == "Contributors":
    rows = run_query("""
        MATCH (c:Contributor)
        RETURN c.name AS name, c.source_file AS source_file,
          count { MATCH (c)-[:CONTRIBUTED]->(p:Paper) }   AS papers,
          count { MATCH (c)-[:CONTRIBUTED]->(m:Method) }  AS methods,
          count { MATCH (c)-[:CONTRIBUTED]->(d:Dataset) } AS datasets
        ORDER BY c.name
    """)
    c_papers = run_query(
        "MATCH (c:Contributor {source_file:$sf})-[:CONTRIBUTED]->(p:Paper) "
        "RETURN p.title AS title, p.doi AS doi, p.field_of_study AS field",
        {"sf": src_file}
    )
elif page == "Search":
    q = st.text_input(
        "Search across papers, methods, and datasets",
        placeholder="e.g. Show me all fusion methods used for Traffic Data."
    )

    if q:
        rgx = f"(?i).*({build_regex(q)}).*"
        params = {"rgx": rgx}

        papers = run_query("""
            MATCH (c:Contributor)-[:CONTRIBUTED]->(p:Paper)
            WHERE p.title =~ $rgx OR p.author =~ $rgx OR p.doi =~ $rgx
               OR p.abstract =~ $rgx OR p.keywords =~ $rgx
            RETURN p.title AS title, p.author AS author,
                   p.doi AS doi, c.name AS contributor
            LIMIT 50
        """, params)

        methods = run_query("""
            MATCH (c:Contributor)-[:CONTRIBUTED]->(m:Method)
            WHERE m.name =~ $rgx OR m.description =~ $rgx OR m.doi =~ $rgx
            RETURN m.name AS name, m.doi AS doi,
                   m.description AS description, c.name AS contributor
            LIMIT 50
        """, params)

        datasets = run_query("""
            MATCH (c:Contributor)-[:CONTRIBUTED]->(d:Dataset)
            WHERE d.data_name =~ $rgx OR d.data_type =~ $rgx
               OR d.doi =~ $rgx OR d.collection_method =~ $rgx
            RETURN d.data_name AS data_name, d.data_type AS data_type,
                   d.doi AS doi, c.name AS contributor
            LIMIT 50
        """, params)