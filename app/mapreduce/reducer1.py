#!/usr/bin/env python3
import sys
from collections import defaultdict


doc_len = {}
doc_meta = {}
term_docs = defaultdict(set)
term_tf = defaultdict(int)

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t")
    rec_type = parts[0]

    if rec_type == "DOC_LEN" and len(parts) >= 3:
        _, doc_id, length = parts[:3]
        doc_len[doc_id] = int(length)

    elif rec_type == "DOC_META" and len(parts) >= 3:
        _, doc_id, title = parts[0], parts[1], "\t".join(parts[2:])
        doc_meta[doc_id] = title

    elif rec_type == "TERM_DF" and len(parts) >= 3:
        _, term, doc_id = parts[:3]
        term_docs[term].add(doc_id)

    elif rec_type == "TERM_TF" and len(parts) >= 4:
        _, term, doc_id, cnt = parts[:4]
        term_tf[(term, doc_id)] += int(cnt)

# Global stats for BM25 and validation
N = len(doc_len)
avgdl = (sum(doc_len.values()) / N) if N else 0.0
print(f"STATS\tDOC_COUNT\t{N}")
print(f"STATS\tAVGDL\t{avgdl}")

for doc_id in sorted(doc_len):
    print(f"DOC_LEN\t{doc_id}\t{doc_len[doc_id]}")

for doc_id in sorted(doc_meta):
    print(f"DOC_META\t{doc_id}\t{doc_meta[doc_id]}")

for term in sorted(term_docs):
    print(f"TERM_DF\t{term}\t{len(term_docs[term])}")

for (term, doc_id) in sorted(term_tf):
    print(f"TERM_TF\t{term}\t{doc_id}\t{term_tf[(term, doc_id)]}")
