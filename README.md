# bnb-block-chunks

GitHub Actions factory for BNB Smart Chain (chainId 56) public block data.

- `workflow_dispatch` inputs: `first_block`, `last_block`, `mode` (`ledger` / `raw`)
- 100-block chunks processed in one job loop (no matrix limits)
- `eth_getBlockByNumber(full=true)` + `eth_getBlockReceipts` -> SQLite (`blocks`, `txs`, `receipts`)
- gzip + manifest -> GitHub Release assets

RPC endpoints (public, no key): `https://bsc-dataseed.bnbchain.org`, `https://bsc-rpc.publicnode.com`.
Public endpoints may rate-limit; script retries and fails over between them.

No private keys, no downloaded third-party data.
