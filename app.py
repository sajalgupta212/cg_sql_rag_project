# app.py
import os, re
import streamlit as st
import pandas as pd
import networkx as nx
from pyvis.network import Network
from agents.mapping_extractor import MappingExtractorAgent
from dotenv import load_dotenv
from datetime import datetime, timedelta
import streamlit.components.v1 as components
import numpy as np
from sentence_transformers import SentenceTransformer
from groq import Groq

st.set_page_config(page_title="SQL Object Lineage & CRUD", layout="wide")
st.title("💬 SQL Object Lineage & CRUD Assistant")

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()
DEFAULT_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "")
DEFAULT_USER = os.getenv("SNOWFLAKE_USER", "")
DEFAULT_ROLE = os.getenv("SNOWFLAKE_ROLE", "")
DEFAULT_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "")
DEFAULT_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "")
DEFAULT_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "")
DEFAULT_PRIVATE_KEY_FILE = os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE", "")
DEFAULT_PRIVATE_KEY_PASSPHRASE = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "")
GROQ_KEY = os.getenv("GROQ_API_KEY")

# -----------------------------
# Sidebar - Snowflake Config
# -----------------------------
st.sidebar.header("Snowflake Config")
sf_account = st.sidebar.text_input("Account", value=DEFAULT_ACCOUNT)
sf_user = st.sidebar.text_input("User", value=DEFAULT_USER)
sf_role = st.sidebar.text_input("Role", value=DEFAULT_ROLE)
sf_warehouse = st.sidebar.text_input("Warehouse", value=DEFAULT_WAREHOUSE)
sf_database = st.sidebar.text_input("Database", value=DEFAULT_DATABASE)
sf_schema = st.sidebar.text_input("Schema", value=DEFAULT_SCHEMA)
sf_private_key_file = st.sidebar.text_input("Private Key File", value=DEFAULT_PRIVATE_KEY_FILE)
sf_private_key_passphrase = st.sidebar.text_input("Private Key Passphrase", type="password", value=DEFAULT_PRIVATE_KEY_PASSPHRASE)

st.sidebar.markdown("---")
st.sidebar.header("Options")
show_lineage = st.sidebar.checkbox("Show Lineage Graph", True)
show_usage_matrix = st.sidebar.checkbox("Show CRUD Usage Matrix", True)
physics_strength = st.sidebar.slider("Graph Physics Strength", 0.5, 5.0, 1.0)

# -----------------------------
# Session state
# -----------------------------
if "agent" not in st.session_state: st.session_state.agent = None
if "global_graph" not in st.session_state: st.session_state.global_graph = None
if "node_sql_map" not in st.session_state: st.session_state.node_sql_map = {}
if "crud_matrix" not in st.session_state: st.session_state.crud_matrix = {}
if "objects" not in st.session_state: st.session_state.objects = {"PROCEDURE": [], "VIEW": [], "TABLE": []}

# -----------------------------
# Connect to Snowflake
# -----------------------------
if st.sidebar.button("Connect"):
    try:
        st.session_state.agent = MappingExtractorAgent()
        if st.session_state.agent.conn:
            st.success("✅ Connected to Snowflake!")
        else:
            st.error("❌ Connection failed")
    except Exception as e:
        st.error(f"❌ Error: {e}")

agent = st.session_state.agent

# -----------------------------
# Display basic Snowflake info
# -----------------------------
if agent and agent.conn:
    try:
        cur = agent.conn.cursor()
        cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE(), CURRENT_ROLE()")
        db, schema, wh, role = cur.fetchone()
        cur.close()
        st.sidebar.markdown("### ❗ Snowflake Info")
        st.sidebar.write(f"**Database:** {db}")
        st.sidebar.write(f"**Schema:** {schema}")
        st.sidebar.write(f"**Warehouse:** {wh}")
        st.sidebar.write(f"**Role:** {role}")
    except Exception as e:
        st.sidebar.error(f"Error fetching Snowflake info: {e}")

# -----------------------------
# Fetch objects from DB
# -----------------------------
def fetch_objects(agent, database, schema, obj_type):
    if not agent or not agent.conn:
        return []
    query = f"""
        SELECT OBJECT_NAME, DDL_TEXT
        FROM "{database}"."{schema}"."DDL_METADATA"
        WHERE OBJECT_DOMAIN = '{obj_type}'
    """
    cur = agent.conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    return [{"name": r[0].upper(), "ddl": r[1]} for r in rows]

def extract_objects_from_ddl(ddl_text):
    if not ddl_text:
        return []
    ddl_text = re.sub(r"--.*", "", ddl_text)
    ddl_text = re.sub(r"\s+", " ", ddl_text)
    return [t.upper() for t in re.findall(r"(?:FROM|JOIN|INSERT INTO|UPDATE|DELETE FROM)\s+([^\s(]+)", ddl_text, re.IGNORECASE)]

# -----------------------------
# Build object-level graph
# -----------------------------
def build_graph(objects_list):
    G = nx.DiGraph()
    node_sql_map = {}
    for obj in objects_list:
        obj_name = obj["name"]
        ddl_text = obj["ddl"]
        refs = extract_objects_from_ddl(ddl_text)
        for ref in refs:
            G.add_edge(obj_name, ref)
            node_sql_map.setdefault(ref, []).append(ddl_text)
            node_sql_map.setdefault(obj_name, []).append(ddl_text)
    return G, node_sql_map

# -----------------------------
# Fetch CRUD usage with timestamps
# -----------------------------
def fetch_crud_usage(agent, database, schema, lookback_days=30):
    if not agent or not agent.conn:
        return {}
    start_time = datetime.utcnow() - timedelta(days=lookback_days)
    query = f"""
        SELECT QUERY_TEXT, START_TIME
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE DATABASE_NAME = '{database}'
          AND SCHEMA_NAME = '{schema}'
          AND START_TIME >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'
        ORDER BY START_TIME DESC
    """
    cur = agent.conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()

    crud_matrix = {}
    for query_text, ts in rows:
        q = query_text.upper()
        for t in re.findall(r"INSERT INTO\s+([^\s(]+)", q): crud_matrix.setdefault(t, {}).setdefault("C", []).append(ts)
        for t in re.findall(r"UPDATE\s+([^\s,]+)", q): crud_matrix.setdefault(t, {}).setdefault("U", []).append(ts)
        for t in re.findall(r"DELETE FROM\s+([^\s,]+)", q): crud_matrix.setdefault(t, {}).setdefault("D", []).append(ts)
        for t in re.findall(r"FROM\s+([^\s,;]+)", q) + re.findall(r"JOIN\s+([^\s,;]+)", q): crud_matrix.setdefault(t, {}).setdefault("R", []).append(ts)

    result = {}
    for obj, ops in crud_matrix.items():
        result[obj] = {
            "Object": obj,
            "C": len(ops.get("C", [])),
            "U": len(ops.get("U", [])),
            "D": len(ops.get("D", [])),
            "R": len(ops.get("R", [])),
            "Execution Timestamps": {k: ops[k] for k in ops},
            "Total CRUD": sum(len(v) for v in ops.values())
        }
    return result

# -----------------------------
# Load DB objects, graph, and CRUD
# -----------------------------
if agent and agent.conn:
    if st.sidebar.button("Load Graph & CRUD"):
        procs = fetch_objects(agent, sf_database, sf_schema, "PROCEDURE")
        views = fetch_objects(agent, sf_database, sf_schema, "VIEW")
        tables = fetch_objects(agent, sf_database, sf_schema, "TABLE")
        all_objects = procs + views + tables
        G, node_sql_map = build_graph(all_objects)
        st.session_state.global_graph = G
        st.session_state.node_sql_map = node_sql_map
        st.session_state.objects = {"PROCEDURE": procs, "VIEW": views, "TABLE": tables}
        st.session_state.crud_matrix = fetch_crud_usage(agent, sf_database, sf_schema)
        st.success(f"✅ Graph loaded with {len(G.nodes)} objects. CRUD fetched for {len(st.session_state.crud_matrix)} objects.")

# -----------------------------
# Sidebar - Display DB Objects
# -----------------------------
if st.session_state.objects:
    st.sidebar.subheader("Database Objects")
    for obj_type in ["TABLE", "VIEW", "PROCEDURE"]:
        objs = [o["name"] for o in st.session_state.objects.get(obj_type, [])]
        st.sidebar.write(f"**{obj_type}s ({len(objs)}):** {', '.join(objs[:10])}{' ...' if len(objs)>10 else ''}")

# -----------------------------
# Lineage Graph
# -----------------------------
if show_lineage and st.session_state.global_graph:
    st.subheader("🌐 Object-Level Lineage Graph")
    obj_type = st.selectbox("Select Object Type", ["PROCEDURE", "VIEW", "TABLE"])
    obj_list = [o["name"] for o in st.session_state.objects.get(obj_type, [])]
    selected_obj = st.selectbox(f"Select {obj_type}", [""] + obj_list)

    if selected_obj:
        G = st.session_state.global_graph
        node_sql_map = st.session_state.node_sql_map
        net = Network(height="650px", width="100%", directed=True)
        net.from_nx(G)
        net.show_buttons(filter_=['physics'])

        for node in net.nodes:
            if node["id"] == selected_obj:
                node["color"] = "orange"
                node["size"] = 25
        for edge in net.edges:
            if edge["from"] == selected_obj or edge["to"] == selected_obj:
                edge["color"] = "red"
                edge["width"] = 3

        for node in net.nodes:
            sql_snippets = node_sql_map.get(node["id"], [])
            node["title"] = "\n\n".join(sql_snippets[:3])

        tmp_path = "temp_graph.html"
        net.save_graph(tmp_path)
        HtmlFile = open(tmp_path, 'r', encoding='utf-8').read()
        components.html(HtmlFile, height=650, scrolling=True)

# -----------------------------
# Dynamic CRUD Matrix
# -----------------------------
if st.session_state.crud_matrix and show_usage_matrix:
    st.subheader("📊 Dynamic CRUD Usage Matrix")

    # Sidebar filters
    obj_type_filter = st.sidebar.multiselect(
        "Object Type", ["TABLE", "VIEW", "PROCEDURE"], default=["TABLE", "VIEW", "PROCEDURE"]
    )
    crud_type_filter = st.sidebar.multiselect(
        "CRUD Type", ["C", "U", "D", "R"], default=["C", "U", "D", "R"]
    )
    start_date = st.sidebar.date_input("Start Date", value=datetime.utcnow() - timedelta(days=30))
    end_date = st.sidebar.date_input("End Date", value=datetime.utcnow())

    # Filtered data
    filtered_objects = []
    for obj_name, ops in st.session_state.crud_matrix.items():
        obj_type = "TABLE"  # default
        for t, lst in st.session_state.objects.items():
            if any(o["name"] == obj_name for o in lst):
                obj_type = t
                break
        if obj_type not in obj_type_filter:
            continue

        filtered_ops = {}
        for crud, times in ops.get("Execution Timestamps", {}).items():
            if crud not in crud_type_filter:
                continue
            filtered_ops[crud] = [ts for ts in times if start_date <= ts.date() <= end_date]

        if filtered_ops:
            filtered_objects.append({
                "Object": obj_name,
                "Type": obj_type,
                **{k: len(v) for k, v in filtered_ops.items()},
                "Total CRUD": sum(len(v) for v in filtered_ops.values()),
                "Execution Timestamps": filtered_ops
            })

    if filtered_objects:
        df = pd.DataFrame(filtered_objects)
        st.dataframe(df)
    else:
        st.info("No CRUD activity found for selected filters.")

# -----------------------------
# ChatGPT-style Q&A
# -----------------------------
st.subheader("💬 Ask Questions about SQL Lineage / CRUD")
question = st.text_area("Enter your question:")

def call_groq_llm(prompt):
    client = Groq(api_key=GROQ_KEY)
    resp = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        temperature=0.1,
        messages=[
            {"role": "system",
             "content": "You are a SQL lineage and CRUD expert. Answer based strictly on provided context."},
            {"role": "user", "content": prompt}
        ]
    )
    return resp.choices[0].message.content

if st.button("Ask"):
    if not agent or not agent.conn:
        st.warning("Connect to Snowflake first")
    else:
        # Combine DDL + CRUD context
        context_lines = []
        for obj_type in ["PROCEDURE", "VIEW", "TABLE"]:
            for obj in st.session_state.objects.get(obj_type, []):
                context_lines.append(f"{obj_type} {obj['name']} DDL:\n{obj['ddl']}\n")
        for obj_name, ops in st.session_state.crud_matrix.items():
            context_lines.append(f"Object {obj_name} CRUD:\n{ops}\n")
        context = "\n".join(context_lines)

        # Embed + retrieve top-k relevant context
        embedder = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
        query_vec = embedder.encode([question])[0]
        vecs = embedder.encode(context_lines)
        sims = np.dot(vecs, query_vec)
        best_idx = int(np.argmax(sims))
        top_context = context_lines[best_idx]

        prompt = f"CONTEXT:\n{top_context}\n\nQUESTION: {question}"
        answer = call_groq_llm(prompt)
        st.markdown(f"**Answer:**\n{answer}")
