#!/usr/bin/env python
import sys

current_key = None
current_type = None

doc_len = {}
term_docs = {}
term_tf = {}

def flush():
    for doc_id, length in doc_len.items():
        print(f"DOC_LEN\t{doc_id}\t{length}")
    for term, docs in term_docs.items():
        print(f"TERM_DF\t{term}\t{len(docs)}")
    for (term, doc_id), tf in term_tf.items():
        print(f"TERM_TF\t{term}\t{doc_id}\t{tf}")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split('\t')
    rec_type = parts[0]

    if rec_type == "DOC_LEN":
        _, doc_id, length = parts
        doc_len[doc_id] = length

    elif rec_type == "TERM_DF":
        _, term, doc_id = parts
        term_docs.setdefault(term, set()).add(doc_id)

    elif rec_type == "TERM_TF":
        _, term, doc_id, cnt = parts
        key = (term, doc_id)
        term_tf[key] = term_tf.get(key, 0) + int(cnt)

flush()