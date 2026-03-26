"""
simple mockup/trial

run with streamlit run app/graph_webapp.py
"""

import streamlit as st
import streamlit.components.v1 as components
from pyvis.network import Network
from utils import connect_to_snowflake

# 1. Page Config
st.set_page_config(layout="wide", page_title="MindMap 2D Debugger")

# 2. State Management
if "selected_pid" not in st.session_state:
    st.session_state.selected_pid = None
if "show_clusters" not in st.session_state:
    st.session_state.show_clusters = False

# Distinct colors for up to 10 clusters
CLUSTER_COLORS = [
    "#e57373", "#64b5f6", "#81c784", "#ffd54f",
    "#ba68c8", "#4db6ac", "#ff8a65", "#90a4ae",
    "#f06292", "#a5d6a7",
]

@st.cache_data(ttl=600)
def fetch_graph_data():
    """fetch quoted data from snowflake."""
    conn = connect_to_snowflake(schema="GOLD")
    cur = conn.cursor()
    try:
        cur.execute('SELECT "source_paper_id", "target_paper_id", "relationship_type", "strength" FROM "GOLD"."GOLD_PAPER_RELATIONSHIPS"')
        edges = cur.fetchall()

        paper_ids = set([s for s, _, _, _ in edges] + [t for _, t, _, _ in edges])
        if not paper_ids: return [], {}

        id_list = ",".join(map(str, paper_ids))
        cur.execute(f'SELECT "id", "title", "abstract", "conclusion" FROM "SILVER"."SILVER_PAPERS" WHERE "id" IN ({id_list})')
        papers = {row[0]: {"title": row[1], "abstract": row[2], "conclusion": row[3]} for row in cur.fetchall()}
        return edges, papers
    finally:
        cur.close()
        conn.close()

@st.cache_data(ttl=600)
def fetch_cluster_data():
    """fetch topic cluster assignments from gold layer."""
    conn = connect_to_snowflake(schema="GOLD")
    cur = conn.cursor()
    try:
        cur.execute('SELECT "paper_id", "cluster_id", "cluster_label", "cluster_name", "cluster_description" FROM "GOLD"."GOLD_PAPER_CLUSTERS"')
        rows = cur.fetchall()
        paper_clusters = {row[0]: {"cluster_id": row[1], "cluster_label": row[2], "cluster_name": row[3] or row[2], "cluster_description": row[4] or ""} for row in rows}
        cluster_labels = {row[1]: row[3] or row[2] for row in rows}
        cluster_descriptions = {row[1]: row[4] or "" for row in rows}
        return paper_clusters, cluster_labels, cluster_descriptions
    finally:
        cur.close()
        conn.close()

def build_2d_graph(edges, papers, focus_id=None, paper_clusters=None, cluster_labels=None):
    """creates a flat 2d graph with colored directional edges."""
    import math, collections

    show_clusters = paper_clusters is not None and len(paper_clusters) > 0

    net = Network(height="700px", width="100%", bgcolor="#ffffff", font_color="#333333", directed=True)

    neighbors = set()
    if focus_id:
        for s, t, _, _ in edges:
            if s == focus_id: neighbors.add(t)
            if t == focus_id: neighbors.add(s)

    # Pre-compute cluster center positions for visual grouping
    cluster_centers = {}
    if show_clusters:
        cluster_ids = sorted({v["cluster_id"] for v in paper_clusters.values()})
        n_clusters = len(cluster_ids)
        radius = 600
        for i, cid in enumerate(cluster_ids):
            angle = (2 * math.pi * i) / n_clusters
            cluster_centers[cid] = (radius * math.cos(angle), radius * math.sin(angle))

        # Count papers per cluster to spread nodes within each cluster
        cluster_members = collections.defaultdict(list)
        for pid, info in paper_clusters.items():
            cluster_members[info["cluster_id"]].append(pid)

    # add nodes
    for p_id, info in papers.items():
        is_focus = (p_id == focus_id)
        is_neighbor = (p_id in neighbors)

        x, y = None, None

        if show_clusters and p_id in paper_clusters:
            cluster_id = paper_clusters[p_id]["cluster_id"]
            bg = CLUSTER_COLORS[cluster_id % len(CLUSTER_COLORS)]
            border = "#333333"
            cluster_label = paper_clusters[p_id]["cluster_label"]
            title_tooltip = f"Cluster {cluster_id}: {paper_clusters[p_id]['cluster_name']}\nKeywords: {cluster_label}\n{info['title']}"

            # Spread nodes within their cluster using a small inner circle
            cx, cy = cluster_centers[cluster_id]
            members = cluster_members[cluster_id]
            idx = members.index(p_id) if p_id in members else 0
            inner_r = 120
            inner_angle = (2 * math.pi * idx) / max(len(members), 1)
            x = cx + inner_r * math.cos(inner_angle)
            y = cy + inner_r * math.sin(inner_angle)
        else:
            bg = "#ff4b4b" if is_focus else ("#e3f2fd" if is_neighbor or not focus_id else "#fafafa")
            border = "#b71c1c" if is_focus else "#1976d2"
            title_tooltip = info['title']

        if is_focus:
            bg = "#ff4b4b"
            border = "#b71c1c"

        node_kwargs = dict(
            label=info['title'][:40] + "...",
            title=title_tooltip,
            shape="box",
            color={"background": bg, "border": border},
            font={'size': 14},
            borderWidth=3 if is_focus else 1,
        )
        if x is not None:
            node_kwargs["x"] = x
            node_kwargs["y"] = y

        net.add_node(p_id, **node_kwargs)

    # add edges with color logic
    for source, target, rel, strength in edges:
        edge_color = "#cccccc"
        opacity = 0.1
        label = ""

        if focus_id:
            if source == focus_id:
                edge_color = "#00bcd4"
                opacity = 0.9
                label = f"out: {rel}"
            elif target == focus_id:
                edge_color = "#9c27b0"
                opacity = 0.9
                label = f"in: {rel}"
        else:
            edge_color = "#2e7d32" if rel == "CITES" else "#f57c00"
            opacity = 0.6
            label = rel

        net.add_edge(
            source, target,
            color={"color": edge_color, "opacity": opacity},
            width=strength * 4 if opacity > 0.5 else 1,
            label=label,
            arrows="to"
        )

    if show_clusters:
        # Enable physics with strong repulsion between clusters, weak within
        net.set_options('''{
            "physics": {
                "enabled": true,
                "barnesHut": {
                    "gravitationalConstant": -3000,
                    "centralGravity": 0.1,
                    "springLength": 120,
                    "springConstant": 0.08,
                    "damping": 0.5
                },
                "stabilization": {"iterations": 150}
            },
            "interaction": {"navigationButtons": true}
        }''')
    else:
        net.set_options('{"physics": {"enabled": false}, "interaction": {"navigationButtons": true}}')

    return net

# --- UI EXECUTION ---
st.title("🧠 MindMap 2D Knowledge Graph")
edges, papers = fetch_graph_data()

if not papers:
    st.error("No papers found in database.")
else:
    col_graph, col_side = st.columns([3, 1])

    with col_side:
        st.subheader("🔍 Inspector")

        # Topic clustering toggle
        if st.button("🗂 Toggle Topic Clusters"):
            st.session_state.show_clusters = not st.session_state.show_clusters

        if st.session_state.show_clusters:
            paper_clusters, cluster_labels, cluster_descriptions = fetch_cluster_data()
            if cluster_labels:
                st.markdown("**Cluster Legend:**")
                for cid, label in sorted(cluster_labels.items()):
                    color = CLUSTER_COLORS[cid % len(CLUSTER_COLORS)]
                    desc = cluster_descriptions.get(cid, "")
                    st.markdown(
                        f'<span style="background:{color};padding:2px 8px;border-radius:4px;margin-right:4px">●</span> **{label}**',
                        unsafe_allow_html=True,
                    )
                    if desc:
                        st.caption(desc)
            else:
                st.warning("No cluster data found. Run the pipeline to generate clusters.")
                paper_clusters, cluster_labels, cluster_descriptions = None, None, {}
        else:
            paper_clusters, cluster_labels, cluster_descriptions = None, None, {}

        st.markdown("---")

        selected_title = st.selectbox(
            "Search Papers:",
            options=["None Selected"] + [p['title'] for p in papers.values()],
            index=0
        )

        current_id = next((pid for pid, d in papers.items() if d['title'] == selected_title), None)
        st.session_state.selected_pid = current_id

        if st.session_state.selected_pid:
            p = papers[st.session_state.selected_pid]
            st.info(f"Viewing Paper ID: {st.session_state.selected_pid}")

            if paper_clusters and st.session_state.selected_pid in paper_clusters:
                c = paper_clusters[st.session_state.selected_pid]
                color = CLUSTER_COLORS[c["cluster_id"] % len(CLUSTER_COLORS)]
                st.markdown(
                    f'<span style="background:{color};padding:2px 8px;border-radius:4px">{c["cluster_name"]}</span>',
                    unsafe_allow_html=True,
                )
                if c.get("cluster_description"):
                    st.caption(c["cluster_description"])

            st.subheader("🔗 Relationships")
            outbound = [e for e in edges if e[0] == st.session_state.selected_pid]
            inbound = [e for e in edges if e[1] == st.session_state.selected_pid]

            with st.expander(f"Outbound References ({len(outbound)})", expanded=True):
                for s, t, rel, strg in outbound:
                    target_t = papers.get(t, {}).get('title', 'Unknown')
                    st.write(f"🔵 **{rel}** ({strg:.2f}) → {target_t[:50]}...")

            with st.expander(f"Inbound Citations/Similarities ({len(inbound)})"):
                for s, t, rel, strg in inbound:
                    source_t = papers.get(s, {}).get('title', 'Unknown')
                    st.write(f"🟣 **{rel}** ({strg:.2f}) ← {source_t[:50]}...")

            st.markdown("---")
            st.subheader("📝 Content")
            st.markdown(f"**{p['title']}**")
            with st.expander("Abstract"):
                st.write(p['abstract'])
            with st.expander("Conclusion"):
                st.write(p['conclusion'])
        else:
            st.info("Select a paper to reveal its network connections.")

    with col_graph:
        pv_net = build_2d_graph(
            edges, papers,
            focus_id=st.session_state.selected_pid,
            paper_clusters=paper_clusters,
            cluster_labels=cluster_labels,
        )
        pv_net.save_graph("graph_static.html")
        with open("graph_static.html", 'r', encoding='utf-8') as f:
            components.html(f.read(), height=750)

if st.button("Refresh Cache"):
    st.cache_data.clear()
    st.rerun()
