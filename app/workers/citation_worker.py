import modal
from typing import Dict, Any

image = (
    modal.Image.debian_slim(python_version="3.10")
    .pip_install("requests", "feedparser", "pymupdf")
)

app = modal.App("mindmap-ml-workers")
# no Snowflake needed yet, but keep secret if you later store refs to DB
secret = modal.Secret.from_name("mindmap-1")


@app.function(image=image, secrets=[secret], timeout=60 * 10)
def get_citations(arxiv_id: str, max_refs: int = 200) -> Dict[str, Any]:
    """
    arXiv-based reference parsing:
      1) Query arXiv API to get metadata + PDF link
      2) Download PDF
      3) Extract text (prefer last pages)
      4) Find References/Bibliography section and split into entries
    """
    import re
    import requests
    import feedparser
    import fitz  # PyMuPDF

    feed_url = f"http://export.arxiv.org/api/query?id_list={arxiv_id}"
    feed = feedparser.parse(feed_url)
    if not feed.entries:
        raise ValueError(f"No arXiv entry found for arxiv_id={arxiv_id}")

    entry = feed.entries[0]
    title = (entry.get("title") or "").strip().replace("\n", " ")
    summary = (entry.get("summary") or "").strip().replace("\n", " ")
    authors = [a.name for a in entry.get("authors", [])]

    pdf_url = None
    for link in entry.get("links", []):
        if getattr(link, "type", None) == "application/pdf":
            pdf_url = link.href
            break
    if not pdf_url:
        pdf_url = f"https://arxiv.org/pdf/{arxiv_id}.pdf"

    meta = {"arxiv_id": arxiv_id, "title": title, "abstract": summary, "authors": authors, "pdf_url": pdf_url}

    r = requests.get(pdf_url, timeout=60)
    r.raise_for_status()
    pdf_bytes = r.content

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")

    # Prefer last pages (references are near the end)
    n_pages = len(doc)
    start = max(0, n_pages - 20)
    full_text = []
    for i in range(start, n_pages):
        full_text.append(doc.load_page(i).get_text("text"))
    text = "\n".join(full_text)

    m = re.search(r"\n\s*(references|bibliography)\s*\n", text, flags=re.IGNORECASE)
    if not m:
        return {"arxiv_metadata": meta, "references": []}

    refs_block = text[m.end():]
    stop = re.search(r"\n\s*(appendix|acknowledg(e)?ments?)\s*\n", refs_block, flags=re.IGNORECASE)
    if stop:
        refs_block = refs_block[:stop.start()]

    chunks = re.split(r"\n\s*(?:\[\d+\]|\d+\.\s|\d+\s)\s*", refs_block)
    refs = []
    for c in chunks:
        c = re.sub(r"\s+", " ", c).strip()
        if len(c) >= 30:
            refs.append(c)

    return {"arxiv_metadata": meta, "references": refs[: int(max_refs)]}
