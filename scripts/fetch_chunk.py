#!/usr/bin/env python3
"""Fetch a BSC block chunk into SQLite: blocks, txs, receipts.

Usage: fetch_chunk.py --first N --last N --out NAME.db --mode raw|ledger
Outputs NAME.db.gz and NAME.db.manifest.json.
"""
import argparse
import datetime as dt
import gzip
import json
import os
import shutil
import sqlite3
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

ENV_RPC = os.environ.get("BSC_RPC", "").strip()
RPCS = [u for u in [ENV_RPC, "https://bsc-dataseed.bnbchain.org",
                    "https://bsc-rpc.publicnode.com"] if u]
CHAIN = "bnb"
CHAIN_ID = 56


def rpc(method, params, tries=5):
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method,
                          "params": params}).encode()
    last = "no rpc"
    for attempt in range(tries):
        for url in RPCS:
            try:
                req = urllib.request.Request(
                    url, payload,
                    {"Content-Type": "application/json",
                     "User-Agent": "bnb-block-chunks/1.0"})
                with urllib.request.urlopen(req, timeout=60) as resp:
                    data = json.load(resp)
                if "error" in data:
                    raise RuntimeError(str(data["error"])[:200])
                if "result" not in data:
                    raise RuntimeError("missing result")
                return data["result"]
            except Exception as exc:  # noqa: BLE001 - retry every transient fault
                last = "%s: %s" % (url, str(exc)[:160])
        time.sleep(min(8, 1 << attempt))
    raise RuntimeError("rpc failed %s: %s" % (method, last))


def fetch_block(number):
    block = rpc("eth_getBlockByNumber", [hex(number), True])
    receipts = rpc("eth_getBlockReceipts", [hex(number)])
    if not isinstance(block, dict) or not isinstance(receipts, list):
        raise RuntimeError("malformed response for block %d" % number)
    txs = block.get("transactions") or []
    if not all(isinstance(t, dict) for t in txs):
        raise RuntimeError("block %d: full transactions not returned" % number)
    return number, block, txs, receipts


def init_db(path):
    con = sqlite3.connect(path)
    con.execute("PRAGMA journal_mode=DELETE")
    con.execute("CREATE TABLE blocks(number INTEGER PRIMARY KEY, hash TEXT, data TEXT)")
    con.execute("CREATE TABLE txs(hash TEXT PRIMARY KEY, block_number INTEGER, idx INTEGER, data TEXT)")
    con.execute("CREATE TABLE receipts(hash TEXT PRIMARY KEY, block_number INTEGER, data TEXT)")
    con.execute("CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT)")
    con.execute("CREATE INDEX txs_block ON txs(block_number)")
    con.execute("CREATE INDEX receipts_block ON receipts(block_number)")
    return con


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--first", type=int, required=True)
    ap.add_argument("--last", type=int, required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--mode", choices=["raw", "ledger"], default="raw")
    ap.add_argument("--workers", type=int, default=2)
    args = ap.parse_args()
    if args.last < args.first:
        ap.error("last < first")

    con = init_db(args.out)
    counts = {"blocks": 0, "txs": 0, "receipts": 0}
    numbers = list(range(args.first, args.last + 1))
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {pool.submit(fetch_block, n): n for n in numbers}
        for fut in as_completed(futures):
            number, block, txs, receipts = fut.result()
            payload = json.dumps(block, separators=(",", ":"))
            con.execute("INSERT OR REPLACE INTO blocks VALUES(?,?,?)",
                        (number, block.get("hash"), payload))
            counts["blocks"] += 1
            for i, tx in enumerate(txs):
                h = tx.get("hash")
                if not h:
                    raise RuntimeError("block %d tx %d missing hash" % (number, i))
                con.execute("INSERT OR REPLACE INTO txs VALUES(?,?,?,?)",
                            (h, number, i, json.dumps(tx, separators=(",", ":"))))
                counts["txs"] += 1
            for i, rc in enumerate(receipts):
                h = rc.get("transactionHash") or (txs[i].get("hash") if i < len(txs) else None)
                if not h:
                    continue
                con.execute("INSERT OR REPLACE INTO receipts VALUES(?,?,?)",
                            (h, number, json.dumps(rc, separators=(",", ":"))))
                counts["receipts"] += 1
            if counts["blocks"] % 10 == 0:
                con.commit()
    if args.mode == "ledger":
        con.execute("CREATE TABLE ledger(hash TEXT PRIMARY KEY, block_number INTEGER, "
                    "from_addr TEXT, to_addr TEXT, value_wei TEXT)")
        con.execute("INSERT OR REPLACE INTO ledger SELECT t.hash, t.block_number, "
                    "json_extract(t.data,'$.from'), json_extract(t.data,'$.to'), "
                    "json_extract(t.data,'$.value') FROM txs t")
    for key, value in [("chain", CHAIN), ("chain_id", str(CHAIN_ID)),
                       ("mode", args.mode), ("rpc", json.dumps(RPCS)),
                       ("methods", "eth_getBlockByNumber,eth_getBlockReceipts")]:
        con.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (key, value))
    con.commit()
    con.close()

    gz = args.out + ".gz"
    with open(args.out, "rb") as src, gzip.open(gz, "wb", compresslevel=9) as dst:
        shutil.copyfileobj(src, dst)
    manifest = {
        "chain": CHAIN, "chain_id": CHAIN_ID,
        "first": args.first, "last": args.last,
        "mode": args.mode, "counts": counts,
        "db_file": os.path.basename(args.out), "db_bytes": os.path.getsize(args.out),
        "gz_file": os.path.basename(gz), "gz_bytes": os.path.getsize(gz),
        "rpc": RPCS,
        "methods": ["eth_getBlockByNumber", "eth_getBlockReceipts"],
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    with open(args.out + ".manifest.json", "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)
    print(json.dumps({k: manifest[k] for k in
                      ("chain", "first", "last", "counts", "db_bytes", "gz_bytes")}))


if __name__ == "__main__":
    sys.exit(main())
