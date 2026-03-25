"""
Simple static visualization of the current knowledge graph edges in Snowflake.
- Connects to Snowflake using environment variables (SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE)
- Queries GOLD_PAPER_RELATIONSHIPS
- Visualizes as a static PNG graph using matplotlib and networkx

Usage:
    python app/simple_vis.py
"""
import matplotlib.pyplot as plt
import networkx as nx
from utils import connect_to_snowflake

def fetch_edges():
    # Use the GOLD schema for the relationships table
    conn = connect_to_snowflake(schema="GOLD")
    cur = conn.cursor()
    try:
        cur.execute('SELECT "source_paper_id", "target_paper_id", "relationship_type", "strength" FROM "GOLD"."GOLD_PAPER_RELATIONSHIPS"')
        edges = cur.fetchall()
    finally:
        cur.close()
        conn.close()
    return edges

def fetch_paper_info(paper_ids):
    # Fetch title, publication year, authors for a set of paper_ids from SILVER_PAPERS
    if not paper_ids:
        return {}
    conn = connect_to_snowflake(schema="SILVER")
    cur = conn.cursor()
    try:
        # Only select the columns we want, and only for the given paper_ids
        format_ids = ','.join(str(int(pid)) for pid in paper_ids)
        query = f'SELECT id, title, reference_list FROM "SILVER"."SILVER_PAPERS" WHERE id IN ({format_ids})'
        cur.execute(query)
        info = {row[0]: {'title': row[1], 'reference_list': row[2]} for row in cur.fetchall()}
    finally:
        cur.close()
        conn.close()
    return info

def visualize_graph(edges):
    G = nx.DiGraph()
    source_ids = set(source for source, _, _, _ in edges)
    paper_info = fetch_paper_info(source_ids)
    for source, target, rel_type, strength in edges:
        # Compose label with title and (optionally) authors or year if available
        info = paper_info.get(source, {})
        label = str(source)
        if info:
            label = info.get('title', str(source))
        G.add_node(source, label=label)
        G.add_node(target, label=str(target))
        G.add_edge(source, target, label=rel_type, weight=strength or 1)

    pos = nx.spring_layout(G, seed=42)
    plt.figure(figsize=(12, 9))
    node_labels = nx.get_node_attributes(G, 'label')
    nx.draw(G, pos, labels=node_labels, node_color='skyblue', edge_color='gray', node_size=700, font_size=9, arrows=True)
    edge_labels = nx.get_edge_attributes(G, 'label')
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_color='red')
    plt.title("Knowledge Graph Edges (GOLD_PAPER_RELATIONSHIPS)\nNode labels: Paper titles (source nodes)")
    plt.tight_layout()
    plt.savefig("graph.png")
    plt.show()
    print("Graph saved to graph.png. Close the plot window to continue.")

def main():
    edges = fetch_edges()
    if not edges:
        print("No edges found in GOLD_PAPER_RELATIONSHIPS.")
        return
    visualize_graph(edges)

if __name__ == "__main__":
    main()
