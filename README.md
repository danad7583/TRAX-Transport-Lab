# TRAX Transport Lab

TRAX Transport Lab is a local integration and test harness for proving that TRAX trust/session events can run above ordinary TCP and UDP carriers without redesigning TRAX Core.

TCP provides byte-stream transport.
TRAX provides trust-session continuity and verifiable event history.
TRAX Transport Lab treats a completed TRAX trust event as a DAG node for this prototype.

## What This Proves

- TCP carries framed bytes only.
- UDP carries datagrams only.
- TRAX admission envelopes verify trust-significant protocol messages above either carrier.
- The lab can enumerate crude DAG nodes from completed TRAX trust events.
- TCP and UDP can run the same TRAX trust sequence and append the same crude DAG event types.

## What This Does Not Prove Yet

This prototype uses local trust-on-first-use public key observation for testing. It does not yet implement production identity discovery, certificates, persistent DAG synchronization, replay storage, HTTPS transport, raw sockets, or RecursiveMAS integration.

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
python -m trax_transport_lab.udp_demo
python -m trax_transport_lab.tcp_demo --json
python -m trax_transport_lab.udp_demo --json
```

Or:

```powershell
python .\scripts\run_all.py
```

Expected TCP demo output includes:

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

Metrics:
transport: tcp
bytes_sent: <number>
bytes_received: <number>
frames_sent: <number>
frames_received: <number>
payload_bytes: <number>
dag_nodes_appended: 2
final_tip: <hash>

Wall-clock:
  total_wall_ms: <number>
  session_handshake_wall_ms: <number>
  stream_exchange_wall_ms: <number>

Event sums, may overlap:
  trax_primitives_event_ms: <number>
  python_packaging_event_ms: <number>
  transport_io_event_ms: <number>
  dag_event_ms: <number>
  orchestration_event_ms: <number>
  unclassified_event_ms: <number>

Micro highlights:
  payload_hash_verify_us: <number>
```

Expected UDP demo output includes:

```text
UDP DEMO OK

TRAX_INIT sent
TRAX_INIT accepted
TRAX_INIT_ACK sent
TRAX_INIT_ACK accepted
TRAX_COMMIT sent
TRAX_COMMIT accepted
SESSION_START_V0 appended

TRAX_REQ sent
TRAX_REQ accepted
TRAX_REQ_ACK sent
TRAX_REQ_ACK accepted
JUNK_STREAM_PAYLOAD sent
JUNK_STREAM_PAYLOAD hash verified
TRAX_RES_ACK sent
TRAX_RES_ACK accepted
STREAM_EXCHANGE_V0 appended

DAG:
0 SESSION_START_V0      <hash>
1 STREAM_EXCHANGE_V0    <hash>

Final tip: <hash>

Metrics:
transport: udp
bytes_sent: <number>
bytes_received: <number>
datagrams_sent: <number>
datagrams_received: <number>
payload_bytes: <number>
dag_nodes_appended: 2
final_tip: <hash>

Wall-clock:
  total_wall_ms: <number>
  session_handshake_wall_ms: <number>
  stream_exchange_wall_ms: <number>

Event sums, may overlap:
  trax_primitives_event_ms: <number>
  python_packaging_event_ms: <number>
  transport_io_event_ms: <number>
  dag_event_ms: <number>
  orchestration_event_ms: <number>
  unclassified_event_ms: <number>

Micro highlights:
  payload_hash_verify_us: <number>
```

## Demo Protocol Sequence

UDP removes TCP's connection handshake but does not remove the need for TRAX's own trust/session handshake. In this lab, TCP and UDP are carriers; TRAX_INIT, TRAX_INIT_ACK, and TRAX_COMMIT establish the TRAX trust context above either carrier.

The logical sequence is the same for TCP frames and UDP datagrams:

1. The carrier is ready. TCP connects; UDP creates a client socket and sends the first datagram.
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

## Metrics

Both demos return a `RunMetrics` object and print a text metrics section. The `--json` option emits machine-readable metrics:

```powershell
python -m trax_transport_lab.tcp_demo
python -m trax_transport_lab.udp_demo
python -m trax_transport_lab.tcp_demo --runs 10
python -m trax_transport_lab.udp_demo --runs 10
python .\scripts\compare_transports.py --runs 10
python -m trax_transport_lab.tcp_demo --json
python -m trax_transport_lab.udp_demo --json
python .\scripts\compare_transports.py --json --runs 10
```

Metrics use `time.perf_counter_ns()` and are intended for local loopback comparison and regression tracking, not benchmark-grade performance claims.

TRAX Transport Lab reports both wall-clock durations and event-sum durations.

Wall-clock durations measure elapsed real time for the demo or major phase.

Event-sum durations add up observed instrumented events. These can overlap across client/server threads, socket waits, and nested operations. Therefore event-sum buckets are useful for visibility, but they are not exclusive slices of total runtime and may exceed `total_wall_ms`.

For example, `transport_io_event_ms` may be greater than `total_wall_ms` because both client and server socket waits are being summed.

The metrics are broken into buckets:

- `trax_primitives`: direct calls into the `trax` Python binding, including `hash32`, nonce generation, session derivation, envelope creation, envelope verification, and envelope decode.
- `python_packaging`: deterministic JSON encoding/decoding, hex conversion, and demo message hashing.
- `transport_io`: TCP frame send/receive and UDP datagram send/receive.
- `dag`: demo DAG append, content hash, node hash, and final-tip operations.
- `orchestration`: demo totals, client/server run totals, thread startup/wait, and high-level sequence spans.
- `unclassified`: wall-clock time not covered by classified event spans.

Use wall-clock metrics to compare end-to-end TCP vs UDP local loopback behavior. Use event-sum metrics to locate where instrumentation observed time being spent. Use micro highlights such as `payload_hash_verify_us` to reason about small TRAX primitive operations.

Stable metric names include:

- `session_handshake_total`
- `TRAX_INIT`
- `TRAX_INIT_ACK`
- `TRAX_COMMIT`
- `stream_exchange_total`
- `TRAX_REQ`
- `TRAX_REQ_ACK`
- `JUNK_STREAM_PAYLOAD`
- `TRAX_RES_ACK`
- `dag_append_SESSION_START_V0`
- `dag_append_STREAM_EXCHANGE_V0`
- `payload_hash_verify`
- `trax.hash32`
- `trax.create_admission_envelope_v1`
- `trax.verify_admission_envelope_v1_for_receiver`
- `message.encode`
- `message.decode`
- `tcp.send_frame`
- `tcp.recv_frame`
- `udp.send_datagram`
- `udp.recv_datagram`
- `dag.append_node`
- `client.total`
- `server.total`
- `demo.total`

TCP metrics include frame counts. UDP metrics include datagram counts. Both include byte totals, payload bytes, DAG nodes appended, final tip, wall-clock durations, event-sum buckets, and micro highlights.

`payload_hash_verify_us` is closer to the light primitive path because it measures the hash-and-compare check for the committed stream payload. If `payload_hash_verify_us` is single-digit microseconds but `total_wall_ms` is tens of milliseconds, the result suggests the hash-binding primitive is light while the Python/demo/transport harness dominates end-to-end timing.

## TCP and UDP Comparison

The current comparison is deliberately narrow:

- TCP uses length-prefixed frames over a byte stream.
- UDP sends one demo message per datagram.
- Both carriers run the same TRAX trust/session sequence.
- Both append `SESSION_START_V0` and `STREAM_EXCHANGE_V0`.
- Both verify the junk payload hash committed by `TRAX_REQ`.
- Both collect local loopback timing and byte-count metrics.

## DAG Semantics

This first pass intentionally models only two crude event nodes:

- `SESSION_START_V0`: completed `TRAX_INIT`, `TRAX_INIT_ACK`, `TRAX_COMMIT`.
- `STREAM_EXCHANGE_V0`: completed `TRAX_REQ`, `TRAX_REQ_ACK`, `JUNK_STREAM_PAYLOAD`, `TRAX_RES_ACK`.

TCP packets and UDP datagrams are not DAG nodes. TRAX security/admission envelopes are the verifiable packet layer. The demo DAG node represents a completed trust-significant event.

## Adverse Test Matrix

The test suite covers:

- malformed `TRAX_INIT`
- wrong receiver key in `TRAX_INIT`
- payload before expected ACK/order point
- junk payload hash mismatch
- wrong `session_id`
- oversized TCP frame
- truncated TCP frame
- UDP timeout / missing packet
- duplicate UDP datagram handling
- wrong DAG previous tip

Additional controlled cases are present in `tcp_demo.py` and `udp_demo.py` for bad init ACK, bad commit, request-before-commit, duplicate datagrams, and wrong message order.

## Known Limitations

- Localhost only.
- UDP does not implement retransmission, fragmentation handling, congestion control, or replay storage.
- No HTTPS, raw sockets, or RecursiveMAS.
- No production identity discovery or PKI.
- No durable DAG storage or replay database.
- The outer JSON message wrapper is demo packaging only, not the security layer.

## Future Roadmap

- Add persistent identity discovery and replay protection.
- Add persistent DAG synchronization experiments.
- Add UDP retransmission experiments and HTTPS transport demos.
- Add richer event semantics once TRAX Core finalizes them.
- Add RecursiveMAS integration after the TCP proof remains stable.

## License

MIT. See [LICENSE](LICENSE).
