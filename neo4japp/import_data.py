#!/usr/bin/env python3
import os
import re
import pandas as pd
from pathlib import Path
from neo4j import GraphDatabase

NEO4J_URI      = "neo4j+s://406508c5.databases.neo4j.io"
NEO4J_USER     = "406508c5"
NEO4J_PASSWORD = "ltBLw-nl5IyRewPm6sjmvcKCfRx2POWsA4b_AfuglKc"
NEO4J_DATABASE = "406508c5"

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
    'publicationdate': 'publication_date', 'publication_date': 'publication_date',
    'url': 'url', 'keywords': 'keywords',
    'abstract': 'abstract', 'publisher': 'publisher',
    'field_of_study': 'field_of_study', 'fieldofstudy': 'field_of_study',
    'isdatafusionpaper': 'is_data_fusion', 'is_datafusion_paper': 'is_data_fusion',
    'datafusionclassificationreason': 'classification_reason',
    'datafusionclassification': 'classification_reason',
    'reason': 'classification_reason',
}

METHOD_COL_MAP = {
    'method_name': 'name', 'methodname': 'name', 'name': 'name',
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
    """, source_file=source_file, **row)


def insert_method(tx, row, source_file):
    tx.run("""
        CREATE (m:Method {
            name: $name, method_key: $method_key, doi: $doi,
            description: $description, u1: $u1, u3: $u3
        })
        WITH m
        MATCH (c:Contributor {source_file: $source_file})
        CREATE (c)-[:CONTRIBUTED]->(m)
    """, source_file=source_file, **row)


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
    """, source_file=source_file, **row)


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