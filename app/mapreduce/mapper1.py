#!/usr/bin/env python
import sys
import re

for line in sys.stdin:
    line = line.strip()
    parts = line.split('\t')
    if len(parts) < 3:
        continue
    doc_id = parts[0]
    doc_title = parts[1]
    text = parts[2]
    words = re.findall(r'\w+', text.lower())
    doc_len = len(words)

    print(f"DOC_LEN\t{doc_id}\t{doc_len}")

    term_counts = {}
    for w in words:
        term_counts[w] = term_counts.get(w, 0) + 1

    for term, cnt in term_counts.items():
        print(f"TERM_TF\t{term}\t{doc_id}\t{cnt}")
        print(f"TERM_DF\t{term}\t{doc_id}")