import streamlit as st
import os
import json
from agents.mapping_extractor import MappingExtractorAgent
from embed_and_store import load_local_embedding_model, embed_texts, store_in_lancedb

st.set_page_config(page_title="SQL RAG Embedding Tool", layout="wide")
st.title("SQL RAG Embedding Tool")

proc_name = st.text_input("Stored Procedure name", value="SALES_DOCUMENT_MASTER_MTLZ_SP")
run_btn = st.button("🚀 Run Full Pipeline")
status_area = st.empty()

if run_btn:
    status_area.info("🔑 Connecting to Snowflake...")
    agent = MappingExtractorAgent()
    status_area.success("✅ Connected to Snowflake successfully.")

    db_name = os.getenv("SNOWFLAKE_DATABASE")
    schema_name = os.getenv("SNOWFLAKE_SCHEMA")

    status_area.info(f"🧠 Fetching DDL for `{proc_name}`...")
    ddl_text = agent.fetch_procedure_text(db_name, schema_name, proc_name)
    if not ddl_text:
        status_area.error("❌ No DDL text returned.")
        st.stop()
    status_area.success("✅ Fetched DDL successfully.")

    status_area.info("📦 Chunking SQL code...")
    chunks = agent.chunk_procedure(ddl_text, max_tokens=150, overlap=25)
    status_area.success(f"✅ Generated {len(chunks)} chunks.")

    status_area.info("🧮 Generating embeddings...")
    model = load_local_embedding_model()
    texts = [c["text"] for c in chunks]
    vectors = embed_texts(texts, model=model)
    status_area.success("✅ Embeddings generated.")

    status_area.info("📁 Storing in LanceDB...")
    store_in_lancedb(chunks, vectors)
    status_area.success("✅ Stored chunks in LanceDB. Pipeline complete!")
