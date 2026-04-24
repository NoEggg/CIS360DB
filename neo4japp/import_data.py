#!/usr/bin/env python3
import os
import re
import pandas as pd
from pathlib import Path
from neo4j import GraphDatabase

NEO4J_URI      = "neo4j+s://eb56ade1.databases.neo4j.io"
NEO4J_USER     = "eb56ade1"
NEO4J_PASSWORD = "EyOEzzUXhcqXtbKtDgt8VDGjpP3nFUZec5M83nhDEzc"
NEO4J_DATABASE = "eb56ade1"

DATA_DIR = os.path.join(os.path.dirname(__file__), "..")

def clean(val):
    """Convert a spreadsheet cell to a clean string, or None if empty."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip().replace('\xa0', ' ').replace('\n', ' ').strip()
    return s if s else None


def contributor_name(filename):
    """Extract a human-readable contributor name from the filename."""
    name = Path(filename).stem
    for suffix in [' Data Fusion', ' Data', ' Dataset Midterm Part 2', 'DataFusionKg']:
        name = name.replace(suffix, '')
    name = name.replace('_', ' ').strip()
    if name and name[0].islower():
        name = name.title()
    return name


def normalize_col(col):
    """Normalize a column header to lowercase with no special characters."""
    if col is None:
        return ''
    col = str(col).strip().lower().replace('\xa0', '').replace(' ', '_')
    return re.sub(r'[^a-z0-9_]', '', col)


PAPER_COL_MAP = {
    'doi': 'doi', 'title': 'title', 'paper_title': 'title',
    'author': 'author', 'authors': 'author', 'name_author': 'author',
    'publication_title': 'publication_title', 'publication': 'publication_title',
    'publicationdate': 'publication_date', 'publication_date': 'publication_date', 'PublicationDate': 'publication_date',
    'url': 'url', 'keywords': 'keywords',
    'abstract': 'abstract', 'publisher': 'publisher',
    'field_of_study': 'field_of_study', 'fieldofstudy': 'field_of_study',
    'isdatafusionpaper': 'is_data_fusion', 'is_datafusion_paper': 'is_data_fusion',
    'datafusionclassificationreason': 'classification_reason',
    'datafusionclassification': 'classification_reason',
    'reason': 'classification_reason',
}

METHOD_COL_MAP = {
    'method_name': 'name', 'methodname': 'name', 'name': 'name', 'Method Name': 'name', 'method name': 'name',
    'method_key': 'method_key', 'methodkey': 'method_key',
    'doi': 'doi', 'doi_method_block': 'doi',
    'description': 'description',
    'u1': 'u1', 'u3': 'u3',
    'uncertainty_type_u1u3': 'u1',
    'uncertainty_type_u1': 'u1', 'uncertainty_type_u3': 'u3',
}

DATASET_COL_MAP = {
    'doi': 'doi', 'doi_dataset_block': 'doi',
    'data_name': 'data_name', 'dataname': 'data_name', 'dataset_name': 'data_name',
    'dataseturl': 'dataset_url', 'dataset_url': 'dataset_url',
    'method_key': 'method_key', 'methodkey': 'method_key',
    'method_key_dataset': 'method_key',
    'data_type': 'data_type', 'datatype': 'data_type',
    'collection_method': 'collection_method', 'collectionmethod': 'collection_method',
    'u2': 'u2', 'u2_dataset': 'u2', 'uncertainty_type_u2': 'u2',
    'spatialcoverage': 'spatial_coverage', 'spatial_coverage': 'spatial_coverage',
    'temporalcoverage': 'temporal_coverage', 'temporal_coverage': 'temporal_coverage',
    'format': 'format', 'license': 'license', 'provenance': 'provenance',
}


def map_columns(df, col_map):
    """Return a rename dict for a DataFrame's columns using the given map."""
    result = {}
    for col in df.columns:
        norm = normalize_col(col)
        if norm in col_map:
            result[col] = col_map[norm]
    return result


def upsert_contributor(tx, name, source_file):
    tx.run(
        "MERGE (c:Contributor {source_file: $source_file}) "
        "ON CREATE SET c.name = $name",
        source_file=source_file, name=name,
    )


def insert_paper(tx, row, source_file):
    tx.run("""
        CREATE (p:Paper {
            doi: $doi, title: $title, author: $author,
            publication_title: $publication_title,
            publication_date: $publication_date,
            url: $url, keywords: $keywords, abstract: $abstract,
            publisher: $publisher, field_of_study: $field_of_study,
            is_data_fusion: $is_data_fusion,
            classification_reason: $classification_reason
        })
        WITH p
        MATCH (c:Contributor {source_file: $source_file})
        CREATE (c)-[:CONTRIBUTED]->(p)
    """, source_file=source_file, doi=row.get('doi'),
    title=row.get('title'),
    author=row.get('author'),
    publication_title=row.get('publication_title'),
    publication_date=row.get('publication_date'),
    url=row.get('url'),
    keywords=row.get('keywords'),
    abstract=row.get('abstract'),
    publisher=row.get('publisher'),
    field_of_study=row.get('field_of_study'),
    is_data_fusion=row.get('is_data_fusion'),
    classification_reason=row.get('classification_reason')
           )


def insert_method(tx, row, source_file):
    tx.run("""
        CREATE (m:Method {
            name: $name, method_key: $method_key, doi: $doi,
            description: $description, u1: $u1, u3: $u3
        })
        WITH m
        MATCH (c:Contributor {source_file: $source_file})
        CREATE (c)-[:CONTRIBUTED]->(m)
    """,
    source_file=source_file,
    name=row.get('name') or row.get('method_name') ,
    method_key=row.get('method_key'),
    doi=row.get('doi'),
    description=row.get('description'),
    u1=row.get('u1'),
    u3=row.get('u3')
    )


def insert_dataset(tx, row, source_file):
    tx.run("""
        CREATE (d:Dataset {
            doi: $doi, data_name: $data_name, dataset_url: $dataset_url,
            method_key: $method_key, data_type: $data_type,
            collection_method: $collection_method, u2: $u2,
            spatial_coverage: $spatial_coverage,
            temporal_coverage: $temporal_coverage,
            format: $format, license: $license, provenance: $provenance
        })
        WITH d
        MATCH (c:Contributor {source_file: $source_file})
        CREATE (c)-[:CONTRIBUTED]->(d)
    """, source_file=source_file, doi=row.get('doi'),
    data_name=row.get('data_name'),
    dataset_url=row.get('dataset_url'),
    method_key=row.get('method_key'),
    data_type=row.get('data_type'),
    collection_method=row.get('collection_method'),
    u2=row.get('u2'),
    spatial_coverage=row.get('spatial_coverage'),
    temporal_coverage=row.get('temporal_coverage'),
    format=row.get('format'),
    license=row.get('license'),
    provenance=row.get('provenance')
           )


def link_by_doi(session):
    session.run("""
        MATCH (p:Paper), (m:Method)
        WHERE p.doi IS NOT NULL AND m.doi IS NOT NULL AND p.doi = m.doi
        MERGE (p)-[:HAS_METHOD]->(m)
    """)
    session.run("""
        MATCH (p:Paper), (d:Dataset)
        WHERE p.doi IS NOT NULL AND d.doi IS NOT NULL AND p.doi = d.doi
        MERGE (p)-[:HAS_DATASET]->(d)
    """)


def import_single_sheet(filepath, session, source_file, is_csv=False):
    """Import data from a single-sheet Excel or CSV file."""
    try:
        if is_csv:
            # Try different encodings
            try:
                df = pd.read_csv(filepath, encoding='utf-8')
            except UnicodeDecodeError:
                try:
                    df = pd.read_csv(filepath, encoding='latin1')
                except:
                    df = pd.read_csv(filepath, encoding='cp1252')
        else:
            df = pd.read_excel(filepath)
    except Exception as e:
        print(f"  Error reading file: {e}")
        return 0, 0, 0

    # Clean column names
    df.columns = [normalize_col(str(col)) for col in df.columns]

    paper_rows = []
    method_rows = []
    dataset_rows = []

    for _, row in df.iterrows():
        # Check if this row has paper data
        paper = {}
        for std_col, src_col in PAPER_COL_MAP.items():
            if src_col in df.columns:
                val = clean(row.get(src_col))
                if val is not None and val != '':
                    paper[std_col] = val
        if paper:  # Only add if has at least one field
            paper_rows.append(paper)

        # Check for method data
        method = {}
        for std_col, src_col in METHOD_COL_MAP.items():
            if src_col in df.columns:
                val = clean(row.get(src_col))
                if val is not None and val != '':
                    method[std_col] = val
        if method:  # Only add if has at least one field
            method_rows.append(method)

        # Check for dataset data
        dataset = {}
        for std_col, src_col in DATASET_COL_MAP.items():
            if src_col in df.columns:
                val = clean(row.get(src_col))
                if val is not None and val != '':
                    dataset[std_col] = val
        if dataset:  # Only add if has at least one field
            dataset_rows.append(dataset)

    # Insert into Neo4j
    for paper in paper_rows:
        try:
            session.execute_write(insert_paper, paper, source_file)
        except Exception as e:
            print(f"    Error inserting paper: {e}")

    for method in method_rows:
        try:
            session.execute_write(insert_method, method, source_file)
        except Exception as e:
            print(f"    Error inserting method: {e}")

    for dataset in dataset_rows:
        try:
            session.execute_write(insert_dataset, dataset, source_file)
        except Exception as e:
            print(f"    Error inserting dataset: {e}")

    return len(paper_rows), len(method_rows), len(dataset_rows)


def import_three_sheet(filepath, session, source_file):
    """Import data from an Excel file with three separate sheets."""
    xls = pd.ExcelFile(filepath)

    paper_rows = []
    method_rows = []
    dataset_rows = []

    for sheet in xls.sheet_names:
        sheet_lower = sheet.lower()
        df = pd.read_excel(filepath, sheet_name=sheet)
        df.columns = [normalize_col(str(col)) for col in df.columns]

        # Look for papers in 'doi' sheet OR sheets with 'paper'
        if sheet_lower == 'doi' or 'paper' in sheet_lower:
            for _, row in df.iterrows():
                paper = {}
                for std_col, src_col in PAPER_COL_MAP.items():
                    if src_col in df.columns:
                        val = clean(row.get(src_col))
                        if val is not None:
                            paper[std_col] = val
                if paper:
                    paper_rows.append(paper)

        # Look for methods in 'fusion_method' sheet OR sheets with 'method'
        elif sheet_lower == 'fusion_method' or 'method' in sheet_lower:
            for _, row in df.iterrows():
                method = {}
                for std_col, src_col in METHOD_COL_MAP.items():
                    if src_col in df.columns:
                        val = clean(row.get(src_col))
                        if val is not None:
                            method[std_col] = val
                if method:
                    method_rows.append(method)

        # Look for datasets in 'data' sheet OR sheets with 'data' or 'dataset'
        elif sheet_lower == 'data' or 'data' in sheet_lower or 'dataset' in sheet_lower:
            for _, row in df.iterrows():
                dataset = {}
                for std_col, src_col in DATASET_COL_MAP.items():
                    if src_col in df.columns:
                        val = clean(row.get(src_col))
                        if val is not None:
                            dataset[std_col] = val
                if dataset:
                    dataset_rows.append(dataset)

    # Insert into Neo4j
    for paper in paper_rows:
        session.execute_write(insert_paper, paper, source_file)
    for method in method_rows:
        session.execute_write(insert_method, method, source_file)
    for dataset in dataset_rows:
        session.execute_write(insert_dataset, dataset, source_file)

    return len(paper_rows), len(method_rows), len(dataset_rows)

def main():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with driver.session(database=NEO4J_DATABASE) as session:
        # Start fresh
        print("Clearing existing graph...")
        session.run("MATCH (n) DETACH DELETE n")

        files = sorted(os.listdir(DATA_DIR))
        excel_files = [f for f in files if f.endswith('.xlsx')]
        csv_files   = [f for f in files if f.endswith('.csv')]

        for filename in excel_files + csv_files:
            filepath = os.path.join(DATA_DIR, filename)
            name = contributor_name(filename)
            session.execute_write(upsert_contributor, name, filename)

            try:
                if filename.endswith('.csv'):
                    p, m, d = import_single_sheet(filepath, session, filename, is_csv=True)
                else:
                    xls = pd.ExcelFile(filepath)
                    if len(xls.sheet_names) >= 2:
                        p, m, d = import_three_sheet(filepath, session, filename)
                    else:
                        p, m, d = import_single_sheet(filepath, session, filename)

                print(f"  {filename}: {p} papers, {m} methods, {d} datasets")
            except Exception as e:
                print(f"  ERROR {filename}: {e}")

        link_by_doi(session)

        # Create indexes for faster lookups
        session.run("CREATE INDEX paper_doi   IF NOT EXISTS FOR (p:Paper)       ON (p.doi)")
        session.run("CREATE INDEX method_doi  IF NOT EXISTS FOR (m:Method)      ON (m.doi)")
        session.run("CREATE INDEX dataset_doi IF NOT EXISTS FOR (d:Dataset)     ON (d.doi)")
        session.run("CREATE INDEX contrib_file IF NOT EXISTS FOR (c:Contributor) ON (c.source_file)")

    driver.close()


if __name__ == '__main__':
    main()
