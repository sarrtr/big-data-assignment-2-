#!/usr/bin/env python3
import re
import sys

TOKEN_RE = re.compile(r"[A-Za-z0-9]+")

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t", 2)
    if len(parts) < 3:
        continue

    doc_id, doc_title, text = parts
    doc_id = doc_id.strip()
    doc_title = doc_title.strip().replace("\t", " ")
    text = text.strip()

    words = TOKEN_RE.findall(text.lower())
    doc_len = len(words)

    print(f"DOC_LEN\t{doc_id}\t{doc_len}")
    print(f"DOC_META\t{doc_id}\t{doc_title}")

    term_counts = {}
    for word in words:
        term_counts[word] = term_counts.get(word, 0) + 1

    for term, cnt in term_counts.items():
        print(f"TERM_TF\t{term}\t{doc_id}\t{cnt}")
        print(f"TERM_DF\t{term}\t{doc_id}")
