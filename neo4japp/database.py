"""
Neo4j connection helper.
The driver is cached so Streamlit reuses one connection across reruns.
"""

import streamlit as st
from neo4j import GraphDatabase

NEO4J_URI      = "neo4j+s://eb56ade1.databases.neo4j.io"   # replace with yours
NEO4J_USER     = "eb56ade1"                                  # replace with yours
NEO4J_PASSWORD = "EyOEzzUXhcqXtbKtDgt8VDGjpP3nFUZec5M83nhDEzc"  # replace with yours
NEO4J_DATABASE = "eb56ade1"                                  # replace with yours


@st.cache_resource
def get_driver():
    """Return a cached Neo4j driver (created once per Streamlit session)."""
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def run_query(query, params=None):
    """Run a Cypher query and return a list of record dicts."""
    driver = get_driver()
    with driver.session(database=NEO4J_DATABASE) as session:
        result = session.run(query, params or {})
        return [dict(record) for record in result]


def run_write(query, params=None):
    """Run a write Cypher query (CREATE / MERGE / SET / DELETE)."""
    driver = get_driver()
    with driver.session(database=NEO4J_DATABASE) as session:
        session.execute_write(lambda tx: tx.run(query, params or {}))

