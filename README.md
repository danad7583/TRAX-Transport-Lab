# TRAX Transport Lab

TRAX Transport Lab is a TCP-only integration and test harness for proving that TRAX trust/session events can run above ordinary TCP sockets without redesigning TRAX Core.

TCP provides byte-stream transport.
TRAX provides trust-session continuity and verifiable event history.
TRAX Transport Lab treats a completed TRAX trust event as a DAG node for this prototype.

## What This Proves

- TCP carries framed bytes only.
- TRAX admission envelopes verify trust-significant protocol messages above TCP.
- The lab can enumerate crude DAG nodes from completed TRAX trust events.

## What This Does Not Prove Yet

This prototype uses local trust-on-first-use public key observation for testing. It does not yet implement production identity discovery, certificates, persistent DAG synchronization, replay storage, UDP transport, HTTPS transport, raw sockets, or RecursiveMAS integration.

## Setup

Install this lab in editable mode:

```powershell
python -m pip install -e .
python -m pip install -r requirements.txt
```

Bootstrap TRAX from GitHub:

```powershell
python .\scripts\bootstrap_trax.py
```

Manual TRAX setup commands:

```powershell
python -m pip install --upgrade pip
python -m pip install maturin pytest

mkdir external
git clone https://github.com/danad7583/TRAX external/TRAX
cd external/TRAX
maturin develop
cd ../..

python -c "import trax; print(trax)"
```

The adapter in `src/trax_transport_lab/trax_adapter.py` isolates direct `trax` calls. The inspected TRAX binding currently exposes the expected API. Its Rust/Python binding default proof type is `"none"`; this lab requests `"direct-ed25519"` and falls back to `"none"` only if the installed binding rejects the label.

## Run

```powershell
python -m pytest
python -m trax_transport_lab.tcp_demo
```

Or:

```powershell
python .\scripts\run_all.py
```

Expected demo output includes:

```text
TCP DEMO OK

TRAX_INIT accepted
TRAX_INIT_ACK accepted
TRAX_COMMIT accepted
SESSION_START_V0 appended

TRAX_REQ accepted
TRAX_REQ_ACK accepted
JUNK_STREAM_PAYLOAD hash verified
TRAX_RES_ACK accepted
STREAM_EXCHANGE_V0 appended

DAG:
0 SESSION_START_V0      <hash>
1 STREAM_EXCHANGE_V0    <hash>

Final tip: <hash>
```

## Demo Protocol Sequence

1. TCP socket connects.
2. Client sends `TRAX_INIT`.
3. Server verifies the TRAX envelope and sends `TRAX_INIT_ACK`.
4. Client verifies `TRAX_INIT_ACK` and sends `TRAX_COMMIT`.
5. Server verifies `TRAX_COMMIT` and appends `SESSION_START_V0`.
6. Client sends `TRAX_REQ`, committing to the junk stream payload hash.
7. Server verifies `TRAX_REQ` and sends `TRAX_REQ_ACK`.
8. Client sends `JUNK_STREAM_PAYLOAD`.
9. Server hashes the stream payload, compares it to the committed hash, sends `TRAX_RES_ACK`, and appends `STREAM_EXCHANGE_V0`.
10. Client verifies `TRAX_RES_ACK`.
11. The demo prints DAG enumeration and the final tip.

## DAG Semantics

This first pass intentionally models only two crude event nodes:

- `SESSION_START_V0`: completed `TRAX_INIT`, `TRAX_INIT_ACK`, `TRAX_COMMIT`.
- `STREAM_EXCHANGE_V0`: completed `TRAX_REQ`, `TRAX_REQ_ACK`, `JUNK_STREAM_PAYLOAD`, `TRAX_RES_ACK`.

TCP packets are not DAG nodes. TRAX security/admission envelopes are the verifiable packet layer. The demo DAG node represents a completed trust-significant event.

## Adverse Test Matrix

The test suite covers:

- malformed `TRAX_INIT`
- wrong receiver key in `TRAX_INIT`
- payload before expected ACK/order point
- junk payload hash mismatch
- wrong `session_id`
- oversized TCP frame
- truncated TCP frame
- wrong DAG previous tip

Additional controlled cases are present in `tcp_demo.py` for bad init ACK, bad commit, request-before-commit, and wrong message order.

## Known Limitations

- Localhost only.
- TCP only.
- No UDP, HTTPS, raw sockets, or RecursiveMAS.
- No production identity discovery or PKI.
- No durable DAG storage or replay database.
- The outer JSON message wrapper is demo packaging only, not the security layer.

## Future Roadmap

- Add persistent identity discovery and replay protection.
- Add persistent DAG synchronization experiments.
- Add UDP and HTTPS transport demos.
- Add richer event semantics once TRAX Core finalizes them.
- Add RecursiveMAS integration after the TCP proof remains stable.

## License

MIT. See [LICENSE](LICENSE).
