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
    "a", "an", "the", "is", "are", "was", "were", "be", "been", "being",
    "in", "on", "at", "to", "for", "of", "with", "by", "from", "as",
    "and", "or", "not", "no", "but", "if", "then", "so", "do", "did",
    "has", "have", "had", "will", "would", "shall", "should", "may",
    "can", "could", "might", "must", "it", "its", "i", "me", "my",
    "we", "us", "our", "you", "your", "he", "she", "they", "them",
    "this", "that", "these", "those", "all", "each", "every", "any",
    "show", "find", "get", "list", "give", "used", "using", "about",
    "data", "fusion", "methods", "papers", "datasets"
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

    st.subheader("📚 Top Fields")
    top_fields = run_query("""
                MATCH (p:Paper)
                WHERE p.field_of_study IS NOT NULL AND p.field_of_study <> ''
                RETURN p.field_of_study AS field, count(p) AS cnt
                ORDER BY cnt DESC LIMIT 10
            """)
    if top_fields:
        st.subheader("Top Fields of Study")
        for field in top_fields:
            st.write(f"{field['field']}: {field['cnt']}")
            
    st.subheader("🏢 Top Publishers")
    top_publishers = run_query("""
        MATCH (p:Paper)
        WHERE p.publisher IS NOT NULL AND p.publisher <> ''
        RETURN p.publisher AS publisher, count(p) AS cnt
        ORDER BY cnt DESC LIMIT 5
    """)
    if top_publishers:
        for publisher in top_publishers:
            st.write(f"**{publisher['publisher']}** - {publisher['cnt']} papers")
    else:
        st.info("No publisher data available")

    st.subheader("🆕 Recent Papers")
    recent_papers = run_query("""
            MATCH (p:Paper)
            WHERE p.publication_date IS NOT NULL
            RETURN p.title AS title, p.publication_date AS date
            ORDER BY p.publication_date DESC LIMIT 5
        """)

    if recent_papers:
        for paper in recent_papers:
            st.write(f"**{paper['title']}** ({paper['date']})")
    else:
        st.info("No recent papers available")


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
        paginated_rows = paginate(rows, per_page=5, key="papers_page")

        options = {f"{r['title'] or r['doi'] or 'Untitled'} [{r['eid'][-6:]}]": r["eid"]
                   for r in paginated_rows[:200]}
        selected_label = st.selectbox("Select a paper", list(options.keys()))
        eid = options[selected_label]

        detail = run_query(
            "MATCH (c:Contributor)-[:CONTRIBUTED]->(p:Paper) WHERE elementId(p)=$eid "
            "RETURN p, c.name AS contributor_name",
            {"eid": eid}
        )

        if detail:
            p = detail[0]['p']

            with st.expander("✏️ Edit this Paper"):
                with st.form("edit_paper_form"):
                    title = st.text_input("Title", value=p.get('title') or "")
                    author = st.text_input("Author", value=p.get('author') or "")
                    doi = st.text_input("DOI", value=p.get('doi') or "")
                    publication_title = st.text_input("Publication Title", value=p.get('publication_title') or "")
                    publication_date = st.text_input("Publication Date", value=p.get('publication_date') or "")
                    url = st.text_input("URL", value=p.get('url') or "")
                    keywords = st.text_input("Keywords", value=p.get('keywords') or "")
                    abstract = st.text_area("Abstract", value=p.get('abstract') or "")
                    publisher = st.text_input("Publisher", value=p.get('publisher') or "")
                    field_of_study = st.text_input("Field of Study", value=p.get('field_of_study') or "")
                    is_data_fusion = st.checkbox("Is Data Fusion?", value=p.get('is_data_fusion') or False)
                    classification_reason = st.text_area("Classification Reason", value=p.get('classification_reason') or "")

                    submitted = st.form_submit_button("💾 Update Paper")
                    if submitted:
                        run_write("""
                            MATCH (p:Paper) WHERE elementId(p) = $eid
                            SET p.title = $title, p.author = $author, p.doi = $doi,
                            p.publication_title = $publication_title,
                            p.publication_date = $publication_date,
                            p.url = $url,
                            p.keywords = $keywords,
                            p.abstract = $abstract,
                            p.publisher = $publisher,
                            p.field_of_study = $field_of_study,
                            p.is_data_fusion = $is_data_fusion,
                            p.classification_reason = $classification_reason
                        """, {"eid": eid, "title": title or None, "author": author or None,
                                "doi": doi or None,
                                "publication_title": publication_title or None,
                                "publication_date": publication_date or None,
                                "url": url or None,
                                "keywords": keywords or None,
                                "abstract": abstract or None,
                                "publisher": publisher or None,
                                "field_of_study": field_of_study or None,
                                "is_data_fusion": is_data_fusion,
                                "classification_reason": classification_reason or None
                              })
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
            author = st.text_input("Author")
            publication_title = st.text_input("Publication Title")
            publication_date = st.text_input("Publication Date")
            url = st.text_input("URL")
            keywords = st.text_input("Keywords")
            abstract = st.text_area("Abstract")
            publisher = st.text_input("Publisher")
            field_of_study = st.text_input("Field of Study")
            is_data_fusion = st.checkbox("Is Data Fusion?")
            classification_reason = st.text_area("Classification Reason")

            contributors = run_query("MATCH (c:Contributor) RETURN c.name AS name")
            contributor_names = [c['name'] for c in contributors] if contributors else []
            selected_contributor = st.selectbox("Contributor", contributor_names) if contributor_names else "No contributors found"

            if st.form_submit_button("➕ Add Paper"):
                run_write("""
                    CREATE (p:Paper {doi:$doi, title:$title, author: $author,
                    publication_title: $publication_title,
                    publication_date: $publication_date,
                    url: $url, 
                    keywords: $keywords, 
                    abstract: $abstract,
                    publisher: $publisher, 
                    field_of_study: $field_of_study,
                    is_data_fusion: $is_data_fusion,
                    classification_reason: $classification_reason
                    })
                """, {"doi": doi or None, "title": title or None, "author": author or None,
                        "publication_title": publication_title or None,
                        "publication_date": publication_date or None,
                        "url": url or None,
                        "keywords": keywords or None,
                        "abstract": abstract or None,
                        "publisher": publisher or None,
                        "field_of_study": field_of_study or None,
                        "is_data_fusion": is_data_fusion,
                        "classification_reason": classification_reason or None
                  })

                if selected_contributor:
                    run_write("""
                                MATCH (c:Contributor {name: $contributor})
                                MATCH (p:Paper {title: $title})
                                CREATE (c)-[:CONTRIBUTED]->(p)
                            """, {
                        "contributor": selected_contributor,
                        "title": title
                    })

                st.success(f"Paper '{title}' added!")
                st.rerun()


elif page == "Methods":
    st.title("⚙️ Methods")

    col1, col2 = st.columns([3, 1])
    with col1:
        q = st.text_input("Search (name, description, DOI)")
    with col2:
        pass

    filters = []
    params = {}
    if q:
        rgx = build_regex(q)
        filters.append(
            "(m.name =~ $rgx OR m.description =~ $rgx OR m.doi =~ $rgx "
            " OR m.method_key =~ $rgx)"
        )
        params["rgx"] = f"(?i).*({rgx}).*"

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    rows = run_query(f"""
        MATCH (c:Contributor)-[:CONTRIBUTED]->(m:Method)
        {where}
        RETURN elementId(m) AS eid, m.name AS name, m.method_key AS method_key,
               m.doi AS doi, m.description AS description, m.u1 AS u1, m.u3 AS u3,
               c.name AS contributor
        ORDER BY m.name
    """, params)

    if rows:
        paginated_rows = paginate(rows, per_page=5, key="methods_page")

        options = {f"{r['name'] or r['method_key'] or 'Unnamed'} [{r['eid'][-6:]}]": r["eid"]
                   for r in paginated_rows[:200]}
        selected_label = st.selectbox("Select a method", list(options.keys()))
        eid = options[selected_label]

        detail = run_query(
            "MATCH (c:Contributor)-[:CONTRIBUTED]->(m:Method) WHERE elementId(m)=$eid "
            "RETURN m, c.name AS contributor_name",
            {"eid": eid}
        )

        if detail:
            m = detail[0]['m']
            contributor_name = detail[0]['contributor_name']

            st.subheader("Method Details")
            st.write(f"**Name:** {m.get('name', 'N/A')}")
            st.write(f"**Method Key:** {m.get('method_key', 'N/A')}")
            st.write(f"**DOI:** {m.get('doi', 'N/A')}")
            st.write(f"**Description:** {m.get('description', 'N/A')}")
            st.write(f"**Uncertainty Type U1:** {m.get('u1', 'N/A')}")
            st.write(f"**Uncertainty Type U3:** {m.get('u3', 'N/A')}")
            st.write(f"**Contributor:** {contributor_name}")

            with st.expander("✏️ Edit this Method"):
                with st.form("edit_method_form"):
                    name = st.text_input("Name", value=m.get('name') or "")
                    method_key = st.text_input("Method Key", value=m.get('method_key') or "")
                    doi = st.text_input("DOI", value=m.get('doi') or "")
                    description = st.text_area("Description", value=m.get('description') or "")
                    u1 = st.text_input("Uncertainty Type U1", value=m.get('u1') or "")
                    u3 = st.text_input("Uncertainty Type U3", value=m.get('u3') or "")

                    submitted = st.form_submit_button("💾 Update Method")
                    if submitted:
                        run_write("""
                            MATCH (m:Method) WHERE elementId(m) = $eid
                            SET m.name = $name, m.method_key = $method_key,
                                m.doi = $doi, m.description = $description,
                                m.u1 = $u1, m.u3 = $u3
                        """, {"eid": eid, "name": name or None, "method_key": method_key or None,
                              "doi": doi or None, "description": description or None,
                              "u1": u1 or None, "u3": u3 or None})
                        st.success("Method updated!")
                        st.rerun()

            if st.button("🗑️ Delete this Method"):
                run_write(
                    "MATCH (m:Method) WHERE elementId(m)=$eid DETACH DELETE m",
                    {"eid": eid}
                )
                st.success("Method deleted.")
                st.rerun()

    st.subheader("➕ Add a New Method")
    with st.form("add_method_form"):
        name = st.text_input("Name")
        method_key = st.text_input("Method Key")
        doi = st.text_input("DOI")
        description = st.text_area("Description")
        u1 = st.text_input("Uncertainty Type U1")
        u3 = st.text_input("Uncertainty Type U3")

        contributors = run_query("MATCH (c:Contributor) RETURN c.name AS name")
        contributor_names = [c['name'] for c in contributors]
        selected_contributor = st.selectbox("Contributor", contributor_names)

        if st.form_submit_button("➕ Add Method"):
            run_write("""
                CREATE (m:Method {
                    name: $name, method_key: $method_key, doi: $doi,
                    description: $description, u1: $u1, u3: $u3
                })
            """, {"name": name or None, "method_key": method_key or None,
                  "doi": doi or None, "description": description or None,
                  "u1": u1 or None, "u3": u3 or None})

            run_write("""
                       MATCH (c:Contributor {name: $contributor})
                       MATCH (m:Method {method_key: $method_key})
                       CREATE (c)-[:CONTRIBUTED]->(m)
                   """, {"contributor": selected_contributor, "method_key": method_key})

            st.success(f"Method '{name}' added!")
            st.rerun()

elif page == "Datasets":
    st.title("🗄️ Datasets")

    col1, col2 = st.columns([3, 1])
    with col1:
        q = st.text_input("Search (data name, type, DOI)")

    filters = []
    params = {}
    if q:
        rgx = build_regex(q)
        filters.append(
            "(d.data_name =~ $rgx OR d.data_type =~ $rgx OR d.doi =~ $rgx "
            " OR d.collection_method =~ $rgx)"
        )
        params["rgx"] = f"(?i).*({rgx}).*"

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    rows = run_query(f"""
        MATCH (c:Contributor)-[:CONTRIBUTED]->(d:Dataset)
        {where}
        RETURN elementId(d) AS eid, 
               d.data_name AS data_name, 
               d.data_type AS data_type,
               d.doi AS doi, 
               d.method_key AS method_key,
               d.collection_method AS collection_method, 
               d.u2 AS u2,
               d.format AS format, 
               d.license AS license, 
               d.provenance AS provenance,
               c.name AS contributor
        ORDER BY d.data_name
    """, params)

    if rows:
        paginated_rows = paginate(rows, per_page=5, key="datasets_page")

        options = {f"{r['data_name'] or r['doi'] or 'Unnamed'} [{r['eid'][-6:]}]": r["eid"]
                   for r in paginated_rows[:200]}
        selected_label = st.selectbox("Select a dataset", list(options.keys()))
        eid = options[selected_label]

        detail = run_query(
            "MATCH (c:Contributor)-[:CONTRIBUTED]->(d:Dataset) WHERE elementId(d)=$eid "
            "RETURN d, c.name AS contributor_name",
            {"eid": eid}
        )

        if detail:
            d = detail[0]['d']
            contributor_name = detail[0]['contributor_name']

            st.subheader("Dataset Details")
            st.write(f"**Data Name:** {d.get('data_name', 'N/A')}")
            st.write(f"**Data Type:** {d.get('data_type', 'N/A')}")
            st.write(f"**DOI:** {d.get('doi', 'N/A')}")
            st.write(f"**Method Key:** {d.get('method_key', 'N/A')}")
            st.write(f"**Collection Method:** {d.get('collection_method', 'N/A')}")
            st.write(f"**Uncertainty Type U2:** {d.get('u2', 'N/A')}")
            st.write(f"**Format:** {d.get('format', 'N/A')}")
            st.write(f"**License:** {d.get('license', 'N/A')}")
            st.write(f"**Provenance:** {d.get('provenance', 'N/A')}")
            st.write(f"**Contributor:** {contributor_name}")

            with st.expander("✏️ Edit this Dataset"):
                with st.form("edit_dataset_form"):
                    data_name = st.text_input("Data Name", value=d.get('data_name') or "")
                    data_type = st.text_input("Data Type", value=d.get('data_type') or "")
                    doi = st.text_input("DOI", value=d.get('doi') or "")
                    method_key = st.text_input("Method Key", value=d.get('method_key') or "")
                    collection_method = st.text_input("Collection Method", value=d.get('collection_method') or "")
                    u2 = st.text_input("Uncertainty Type U2", value=d.get('u2') or "")
                    format = st.text_input("Format", value=d.get('format') or "")
                    license = st.text_input("License", value=d.get('license') or "")
                    provenance = st.text_input("Provenance", value=d.get('provenance') or "")

                    submitted = st.form_submit_button("💾 Update Dataset")
                    if submitted:
                        run_write("""
                            MATCH (d:Dataset) WHERE elementId(d) = $eid
                            SET d.data_name = $data_name, 
                                d.data_type = $data_type,
                                d.doi = $doi, 
                                d.method_key = $method_key, 
                                d.collection_method = $collection_method,
                                d.u2 = $u2, 
                                d.format = $format, 
                                d.license = $license, 
                                d.provenance = $provenance
                        """, {"eid": eid, "data_name": data_name or None, "data_type": data_type or None,
                              "doi": doi or None, "method_key": method_key or None,
                              "collection_method": collection_method or None,
                              "u2": u2 or None, "format": format or None,
                              "license": license or None, "provenance": provenance or None})
                        st.success("Dataset updated!")
                        st.rerun()

            if st.button("🗑️ Delete this Dataset"):
                run_write(
                    "MATCH (d:Dataset) WHERE elementId(d)=$eid DETACH DELETE d",
                    {"eid": eid}
                )
                st.success("Dataset deleted.")
                st.rerun()

    st.subheader("➕ Add a New Dataset")
    with st.form("add_dataset_form"):
        data_name = st.text_input("Data Name")
        data_type = st.text_input("Data Type")
        doi = st.text_input("DOI")
        method_key = st.text_input("Method Key")
        collection_method = st.text_input("Collection Method")
        u2 = st.text_input("Uncertainty Type U2")
        format = st.text_input("Format")
        license = st.text_input("License")
        provenance = st.text_input("Provenance")

        contributors = run_query("MATCH (c:Contributor) RETURN c.name AS name")
        contributor_names = [c['name'] for c in contributors]
        selected_contributor = st.selectbox("Contributor", contributor_names)

        if st.form_submit_button("➕ Add Dataset"):
            run_write("""
                CREATE (d:Dataset {
                    data_name: $data_name, 
                    data_type: $data_type, 
                    doi: $doi,
                    method_key: $method_key,
                    collection_method: $collection_method, 
                    u2: $u2,
                    format: $format, 
                    license: $license, 
                    provenance: $provenance
                })
            """, {"data_name": data_name or None, "data_type": data_type or None,
                  "doi": doi or None, "method_key": method_key or None,
                  "collection_method": collection_method or None, "u2": u2 or None,
                  "format": format or None, "license": license or None,
                  "provenance": provenance or None})

            run_write("""
                        MATCH (c:Contributor {name: $contributor})
                        MATCH (d:Dataset {data_name: $data_name})
                        CREATE (c)-[:CONTRIBUTED]->(d)
                    """, {"contributor": selected_contributor, "data_name": data_name})

            st.success(f"Dataset '{data_name}' added!")
            st.rerun()

elif page == "Contributors":
    st.title("👥 Contributors")

    rows = run_query("""
        MATCH (c:Contributor)
        RETURN c.name AS name, c.source_file AS source_file,
          count { MATCH (c)-[:CONTRIBUTED]->(p:Paper) } AS papers,
          count { MATCH (c)-[:CONTRIBUTED]->(m:Method) } AS methods,
          count { MATCH (c)-[:CONTRIBUTED]->(d:Dataset) } AS datasets
        ORDER BY c.name
    """)

    if rows:

        for contributor in rows:
            st.write(f"**{contributor['name']}**")
            st.write(
                f"  📄 Papers: {contributor['papers']} | ⚙️ Methods: {contributor['methods']} | 🗄️ Datasets: {contributor['datasets']}")
            st.divider()

        st.subheader("View Contributor Details")
        contributor_names = [r['name'] for r in rows]
        selected_name = st.selectbox("Select a contributor", contributor_names)

        selected = next(r for r in rows if r['name'] == selected_name)

        st.subheader(f"📊 Contributions by {selected_name}")

        col1, col2, col3 = st.columns(3)
        col1.metric("Papers", selected['papers'])
        col2.metric("Methods", selected['methods'])
        col3.metric("Datasets", selected['datasets'])

        if selected['papers'] > 0:
            st.write("**📄 Papers:**")
            papers = run_query(
                "MATCH (c:Contributor {source_file:$sf})-[:CONTRIBUTED]->(p:Paper) "
                "RETURN p.title AS title, p.doi AS doi, p.publication_date AS date",
                {"sf": selected['source_file']}
            )
            for paper in papers:
                st.write(f"  • {paper['title']} - {paper.get('doi', 'No DOI')}")

        if selected['methods'] > 0:
            st.write("**⚙️ Methods:**")
            methods = run_query(
                "MATCH (c:Contributor {source_file:$sf})-[:CONTRIBUTED]->(m:Method) "
                "RETURN m.name AS name, m.method_key AS key",
                {"sf": selected['source_file']}
            )
            for method in methods:
                st.write(f"  • {method['name']} ({method['key']})")

        if selected['datasets'] > 0:
            st.write("**🗄️ Datasets:**")
            datasets = run_query(
                "MATCH (c:Contributor {source_file:$sf})-[:CONTRIBUTED]->(d:Dataset) "
                "RETURN d.data_name AS name, d.data_type AS type",
                {"sf": selected['source_file']}
            )
            for dataset in datasets:
                st.write(f"  • {dataset['name']} - {dataset['type']}")
    else:
        st.info("No contributors found")

elif page == "Search":
    st.title("🔍 Search")

    st.subheader("Quick Search")
    q = st.text_input(
        "Search across papers, methods, and datasets",
        placeholder="e.g. Show me all fusion methods used for Traffic Data."
    )

    if q:
        rgx = f"(?i).*({build_regex(q)}).*"
        params = {"rgx": rgx}

        q_lower = q.lower()

        show_papers = 'paper' in q_lower or 'publication' in q_lower
        show_methods = 'method' in q_lower or 'technique' in q_lower or 'algorithm' in q_lower
        show_datasets = 'dataset' in q_lower or 'data' in q_lower

        if not (show_papers or show_methods or show_datasets):
            show_papers = show_methods = show_datasets = True

        papers = []
        methods = []
        datasets = []

        if show_papers:
            papers = run_query("""
                MATCH (c:Contributor)-[:CONTRIBUTED]->(p:Paper)
                WHERE p.title =~ $rgx OR p.author =~ $rgx OR p.doi =~ $rgx
                   OR p.abstract =~ $rgx OR p.keywords =~ $rgx
                   OR p.publisher =~ $rgx OR p.publication_title =~ $rgx
                RETURN p.title AS title, p.author AS author,
                       p.doi AS doi, c.name AS contributor
                LIMIT 50
            """, params)

        if show_methods:
            methods = run_query("""
                MATCH (c:Contributor)-[:CONTRIBUTED]->(m:Method)
                WHERE m.name =~ $rgx OR m.description =~ $rgx OR m.doi =~ $rgx
                   OR EXISTS {
                       MATCH (m)<-[:HAS_METHOD]-(p:Paper)-[:HAS_DATASET]->(d:Dataset)
                       WHERE d.data_name =~ $rgx OR d.data_type =~ $rgx OR d.collection_method =~ $rgx
                   }
                RETURN DISTINCT m.name AS name, m.doi AS doi,
                       m.description AS description, c.name AS contributor
                LIMIT 50
            """, params)

        if show_datasets:
            datasets = run_query("""
                MATCH (c:Contributor)-[:CONTRIBUTED]->(d:Dataset)
                WHERE d.data_name =~ $rgx OR d.data_type =~ $rgx
                   OR d.doi =~ $rgx OR d.collection_method =~ $rgx
                RETURN d.data_name AS data_name, d.data_type AS data_type,
                       d.doi AS doi, c.name AS contributor
                LIMIT 50
            """, params)

        st.subheader("📄 Results")

        if papers:
            st.write(f"**Papers Found: {len(papers)}**")
            for paper in papers:
                with st.container():
                    st.write(f"**{paper.get('title', 'No title')}**")
                    st.write(f"  Author: {paper.get('author', 'Unknown')} | DOI: {paper.get('doi', 'N/A')}")
                    st.write(f"  Contributor: {paper.get('contributor', 'Unknown')}")
                    st.divider()
        elif show_papers:
            st.info("No papers found")

        # Show Methods
        if methods:
            st.write(f"**Methods Found: {len(methods)}**")
            for method in methods:
                with st.container():
                    st.write(f"**{method.get('name', 'No name')}**")
                    st.write(f"  DOI: {method.get('doi', 'N/A')}")
                    st.write(f"  Description: {method.get('description', 'N/A')[:200]}...")
                    st.write(f"  Contributor: {method.get('contributor', 'Unknown')}")
                    st.divider()
        elif show_methods:
            st.info("No methods found")

        # Show Datasets
        if datasets:
            st.write(f"**Datasets Found: {len(datasets)}**")
            for dataset in datasets:
                with st.container():
                    st.write(f"**{dataset.get('data_name', 'No name')}**")
                    st.write(f"  Type: {dataset.get('data_type', 'N/A')} | DOI: {dataset.get('doi', 'N/A')}")
                    st.write(f"  Contributor: {dataset.get('contributor', 'Unknown')}")
                    st.divider()
        elif show_datasets:
            st.info("No datasets found")

    else:
        st.info("Enter a search term above to find papers, methods, and datasets")

    st.divider()

    with st.expander("🔬 Advanced Graph Queries (Linkage, Uncertainty, Discovery)", expanded=False):
        st.markdown("*These queries demonstrate the power of graph database traversal and analysis.*")

        tab1, tab2, tab3 = st.tabs(["🔗 Linkage Query", "📊 Uncertainty Query", "⭐ Discovery Query"])

        with tab1:
            st.subheader("Find Methods Applied to Two Datasets")
            st.markdown("Discover fusion methods that work with **both** selected datasets.")

            all_datasets = run_query("""
                MATCH (d:Dataset)
                WHERE d.data_name IS NOT NULL
                RETURN d.data_name AS name, d.data_type AS type
                ORDER BY d.data_name
            """)

            if len(all_datasets) >= 2:
                dataset_names = [d['name'] for d in all_datasets]

                col1, col2 = st.columns(2)
                with col1:
                    dataset_a = st.selectbox("Dataset A", dataset_names, key="linkage_a")
                with col2:
                    dataset_b = st.selectbox("Dataset B", dataset_names, key="linkage_b")

                if dataset_a != dataset_b:
                    if st.button("🔍 Find Common Methods", key="linkage_btn", use_container_width=True):
                        with st.spinner("Traversing graph relationships..."):
                            linkage_query = """
                                MATCH (d1:Dataset {data_name: $dataset_a})<-[:HAS_DATASET]-(p1:Paper)-[:HAS_METHOD]->(m:Method)
                                MATCH (d2:Dataset {data_name: $dataset_b})<-[:HAS_DATASET]-(p2:Paper)-[:HAS_METHOD]->(m)
                                RETURN DISTINCT 
                                    m.name AS method_name,
                                    m.method_key AS method_key,
                                    m.description AS description,
                                    collect(DISTINCT p1.title)[0..3] AS example_papers
                                ORDER BY m.name
                            """
                            results = run_query(linkage_query, {"dataset_a": dataset_a, "dataset_b": dataset_b})

                            if results:
                                st.success(f"✅ Found {len(results)} method(s) applied to both datasets!")
                                for r in results:
                                    with st.expander(f"🔧 {r['method_name']}"):
                                        st.write(f"**Method Key:** {r['method_key']}")
                                        st.write(f"**Description:** {r['description'][:300]}...")
                                        if r['example_papers']:
                                            st.write(f"**Example Papers:** {', '.join(r['example_papers'])}")
                            else:
                                st.info(f"No common methods found between '{dataset_a}' and '{dataset_b}'")
                else:
                    st.warning("Please select two different datasets")
            else:
                st.warning(f"Need at least 2 datasets. Found {len(all_datasets)}.")

        with tab2:
            st.subheader("Find Papers by Uncertainty Type")
            st.markdown("Search for papers reporting **U2 (Measurement)** uncertainty for specific data types.")

            data_types = run_query("""
                MATCH (d:Dataset)
                WHERE d.data_type IS NOT NULL AND d.data_type <> ''
                RETURN DISTINCT d.data_type AS data_type
                ORDER BY d.data_type
            """)

            if data_types:
                type_options = [dt['data_type'] for dt in data_types]
                selected_type = st.selectbox("Select Data/Sensor Type", type_options, key="uncertainty_type")

                if st.button("📊 Find U2 Uncertainties", key="uncertainty_btn", use_container_width=True):
                    with st.spinner("Filtering by uncertainty type..."):
                        uncertainty_query = """
                            MATCH (d:Dataset)<-[:HAS_DATASET]-(p:Paper)
                            WHERE toLower(d.data_type) CONTAINS toLower($data_type)
                              AND d.u2 IS NOT NULL 
                              AND d.u2 <> ''
                            RETURN DISTINCT
                                   p.title AS paper_title,
                                   p.doi AS doi,
                                   d.data_name AS dataset_name,
                                   d.u2 AS uncertainty_description,
                                   d.collection_method AS collection_method
                            ORDER BY p.title
                            LIMIT 20
                        """
                        results = run_query(uncertainty_query, {"data_type": selected_type})

                        if results:
                            st.success(f"✅ Found {len(results)} paper(s) with U2 uncertainty for '{selected_type}'")
                            for r in results:
                                with st.expander(f"📄 {r['paper_title']}"):
                                    st.write(f"**DOI:** {r['doi']}")
                                    st.write(f"**Dataset:** {r['dataset_name']}")
                                    st.write(f"**Collection Method:** {r.get('collection_method', 'N/A')}")
                                    st.write(f"**📏 U2 Uncertainty:** {r['uncertainty_description']}")
                        else:
                            st.info(f"No papers found with U2 uncertainty for '{selected_type}'")
            else:
                st.info("No data types found in the database")

        with tab3:
            st.subheader("Find Most Popular Dataset")
            st.markdown("Discover which dataset has the most connections to different fusion methods.")

            if st.button("⭐ Discover Most Popular Dataset", key="discovery_btn", use_container_width=True):
                with st.spinner("Analyzing graph connections and aggregating results..."):
                    popularity_query = """
                        MATCH (d:Dataset)<-[:HAS_DATASET]-(p:Paper)-[:HAS_METHOD]->(m:Method)
                        RETURN 
                            d.data_name AS dataset_name,
                            d.data_type AS data_type,
                            count(DISTINCT m) AS method_count,
                            collect(DISTINCT m.name)[0..5] AS sample_methods,
                            count(DISTINCT p) AS paper_count
                        ORDER BY method_count DESC
                        LIMIT 5
                    """
                    results = run_query(popularity_query)

                    if results:
                        top = results[0]
                        st.success(
                            f"🏆 **Most Popular Dataset:** '{top['dataset_name']}' with {top['method_count']} different methods!")

                        st.subheader("📊 Top 5 Most Connected Datasets")

                        for i, r in enumerate(results, 1):
                            col1, col2, col3 = st.columns([2, 1, 1])
                            with col1:
                                if i == 1:
                                    st.write(f"**🥇 #{i}** - {r['dataset_name']}")
                                elif i == 2:
                                    st.write(f"**🥈 #{i}** - {r['dataset_name']}")
                                elif i == 3:
                                    st.write(f"**🥉 #{i}** - {r['dataset_name']}")
                                else:
                                    st.write(f"**#{i}** - {r['dataset_name']}")
                            with col2:
                                st.write(f"🔧 {r['method_count']} methods")
                            with col3:
                                st.write(f"📄 {r['paper_count']} papers")

                            if r['sample_methods']:
                                st.caption(f"Sample methods: {', '.join(r['sample_methods'][:3])}")
                            st.divider()

                        st.info(
                            "💡 **Graph Insight:** This query demonstrates aggregation in a graph database - counting connections from datasets to methods through papers.")
                    else:
                        st.info("No relationships found between datasets and methods")

