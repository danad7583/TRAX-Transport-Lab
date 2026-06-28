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
python -m trax_transport_lab.tcp_demo --json --include-events
python -m trax_transport_lab.udp_demo --json --include-events
python -m trax_transport_lab.tcp_demo --mode signed-envelope
python -m trax_transport_lab.tcp_demo --mode checkpoint
python -m trax_transport_lab.udp_demo --mode signed-envelope
python -m trax_transport_lab.udp_demo --mode checkpoint
python -m trax_transport_lab.tcp_demo --mode dag-genesis
python -m trax_transport_lab.udp_demo --mode dag-genesis
```

Or:

```powershell
python .\scripts\run_all.py
```

`run_all.py` validates both the signed-envelope baseline and the DAG-genesis intended hot-path model. It runs the default TCP/UDP demos for backward compatibility, then runs the DAG-genesis TCP/UDP demos, signed-envelope vs DAG-genesis mode comparisons, and a DAG-genesis TCP-vs-UDP comparison.

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

Primitive highlights:
  payload_hash_verify_us: <number>
  dag_append_event_us: <number>
  trax_hash32_event_us: <number>
  trax_create_envelope_event_ms: <number>
  trax_verify_envelope_event_ms: <number>

Counts:
bytes_sent: <number>
bytes_received: <number>
frames_sent: <number>
frames_received: <number>
payload_bytes: <number>
dag_nodes_appended: 2

Slowest events:
1. client.total [orchestration] <number> ms
2. server.total [orchestration] <number> ms

Interpretation:
local loopback diagnostic metrics; not benchmark-grade results
wall-clock values measure elapsed demo time
event-sum values may overlap across client/server threads and nested operations
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

Primitive highlights:
  payload_hash_verify_us: <number>
  dag_append_event_us: <number>
  trax_hash32_event_us: <number>
  trax_create_envelope_event_ms: <number>
  trax_verify_envelope_event_ms: <number>

Counts:
bytes_sent: <number>
bytes_received: <number>
datagrams_sent: <number>
datagrams_received: <number>
payload_bytes: <number>
dag_nodes_appended: 2

Slowest events:
1. client.total [orchestration] <number> ms
2. server.total [orchestration] <number> ms

Interpretation:
local loopback diagnostic metrics; not benchmark-grade results
wall-clock values measure elapsed demo time
event-sum values may overlap across client/server threads and nested operations
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

## Signed-envelope mode vs checkpoint mode

TRAX Transport Lab can run the same TCP and UDP demos in two experimental modes:

- `signed-envelope` / signed-envelope-mode signs every TRAX protocol message envelope. This is conservative and easy to reason about, but expensive.
- `checkpoint` / checkpoint-mode signs session/checkpoint state and hash-binds intermediate events into the DAG.

Checkpoint mode demonstrates the intended TRAX performance model: packets carry evidence, DAGs carry continuity, signatures seal checkpoints.

Run the modes directly:

```powershell
python -m trax_transport_lab.udp_demo --mode signed-envelope
python -m trax_transport_lab.udp_demo --mode checkpoint
python .\scripts\compare_modes.py --runs 10
python .\scripts\compare_modes.py --transport udp --runs 10
python .\scripts\compare_transports.py --mode checkpoint --runs 10
```

In checkpoint mode, the stream messages are hash-bound, counter-bound, session-bound, previous-tip-bound, and DAG-bound. The final checkpoint signs a compact summary of the segment and appends `CHECKPOINT_V0`.

If checkpoint mode reduces signed envelope create/verify event time while preserving payload hash verification, DAG continuity, and final checkpoint verification, then the lab demonstrates why TRAX should sign continuity checkpoints instead of every packet.

These are local loopback diagnostic metrics, not benchmark-grade claims.

## DAG Genesis Mode

DAG-genesis mode is the intended TRAX security model. It signs the genesis DAG authority once, then accepts later packets only when their hash-bound contents extend the DAG through valid continuity.

TRAX security is the DAG. Packets are not trusted because they are individually signed. Packets are accepted only when their hash-bound content can extend the signed DAG genesis through valid continuity.

Content is hashed. DAG state is signed. Continuity is verified.

Packets carry evidence. The DAG carries trust.

Run DAG-genesis mode directly:

```powershell
python -m trax_transport_lab.tcp_demo --mode dag-genesis
python -m trax_transport_lab.udp_demo --mode dag-genesis
python -m trax_transport_lab.udp_demo --mode dag-genesis --json
python .\scripts\compare_modes.py --mode-a signed-envelope --mode-b dag-genesis --transport udp --runs 10
python .\scripts\compare_modes.py --mode-a signed-envelope --mode-b dag-genesis --transport tcp --runs 10
python .\scripts\compare_transports.py --mode dag-genesis --runs 10
```

Validate DAG-genesis mode:

```powershell
python -m trax_transport_lab.tcp_demo --mode dag-genesis
python -m trax_transport_lab.udp_demo --mode dag-genesis
python .\scripts\compare_modes.py --mode-a signed-envelope --mode-b dag-genesis --transport udp --runs 10
python .\scripts\compare_modes.py --mode-a signed-envelope --mode-b dag-genesis --transport tcp --runs 10
python .\scripts\compare_transports.py --mode dag-genesis --runs 10
```

Expected DAG-genesis metrics:

```text
hot_path_signed_packet_count: 0
signed_genesis_create_count: 1
signed_genesis_verify_count: 1
hash_bound_message_count: > 0
```

In DAG-genesis mode, `hot_path_signed_packet_count` must be 0. The only signature operation in the normal run is genesis signing/verification. Hash-bound messages and DAG append operations form the hot path.

These are local loopback diagnostic metrics, not benchmark-grade claims.

## Scaled Message Runs and Cadence Injection

Scaled runs measure how DAG-genesis behaves as normal post-genesis message count grows. The lab can inject DAG segment signing cadence, external agent key rotation cadence, and internal DAG key rotation cadence to model long-running sessions.

Normal AAIP messages remain unsigned in `dag-genesis` mode. The scaled path uses lab-level simulated DAG segment proofs and key-rotation events when Rust-backed primitives are not exposed, and reports those simulations with explicit metric flags such as `dag_segment_proof_simulated`, `agent_key_rotation_simulated`, `dag_key_rotation_simulated`, and `key_mode_simulated`.

There are two key-rotation domains:

1. Agent key rotation: external agent identity/public-key update. It is recorded as `AGENT_KEY_ROTATION_V0`, may use a rare signed AAIP/security update packet, and is not a new genesis node.
2. DAG/internal key rotation: internal TRAX DAG signing authority rotation. It is controlled by DAG/Rust configuration when available, recorded as `DAG_KEY_ROTATION_V0`, and separate from external agent identity key rotation.

Run scaled message comparisons:

```powershell
python .\scripts\scale_messages.py --mode dag-genesis

python .\scripts\scale_messages.py --transport udp --mode dag-genesis --counts 10 100 1000 --runs 3

python .\scripts\scale_messages.py --transport udp --mode dag-genesis --counts 1000 --dag-signing-cadence 8 --agent-key-rotation-cadence 100 --dag-key-rotation-cadence 1000 --key-mode separate --max-dag-nodes 100000 --runs 3

python .\scripts\compare_modes.py --mode-a signed-envelope --mode-b dag-genesis --transport udp --messages 1000 --dag-signing-cadence 8 --agent-key-rotation-cadence 100 --dag-key-rotation-cadence 1000 --key-mode separate --max-dag-nodes 100000 --runs 3
```

The demos also accept these knobs directly:

```powershell
python -m trax_transport_lab.udp_demo --mode dag-genesis --messages 1000 --dag-signing-cadence 8 --agent-key-rotation-cadence 100 --dag-key-rotation-cadence 0 --key-mode separate --max-dag-nodes 100000

python -m trax_transport_lab.tcp_demo --mode dag-genesis --messages 1000 --dag-signing-cadence 8 --key-rotation-cadence 100 --dag-key-rotation-cadence 1000 --key-mode derived --max-dag-nodes 100000 --seal-final-partial
```

In DAG-genesis mode:

- `hot_path_signed_packet_count` should remain 0.
- `signed_genesis_create_count` should remain 1.
- `signed_genesis_verify_count` should remain 1.
- `hash_bound_message_count` should grow with messages.
- `dag_segment_count` should grow with messages / `dag_signing_cadence`.
- `agent_key_rotation_event_count` should grow with messages / `agent_key_rotation_cadence`.
- `dag_key_rotation_event_count` should grow with DAG node count / `dag_key_rotation_cadence`.

`max_dag_nodes` controls how many DAG nodes/events are retained in memory during scaled runs. The lab keeps the current tip available, reports `dag_nodes_retained`, `dag_nodes_pruned`, and `dag_prune_count`, and accepts values of 100000 and higher for long-run diagnostics.

These are local loopback diagnostic metrics, not benchmark-grade claims.

## Metrics

Both demos return a `RunMetrics` object and print a text metrics section. The `--json` option emits machine-readable metrics:

```powershell
python -m trax_transport_lab.tcp_demo
python -m trax_transport_lab.udp_demo
python -m trax_transport_lab.tcp_demo --runs 10
python -m trax_transport_lab.udp_demo --runs 10
python .\scripts\compare_transports.py --runs 10
python .\scripts\compare_transports.py --mode checkpoint --runs 10
python .\scripts\compare_modes.py --runs 10
python .\scripts\compare_modes.py --mode-a signed-envelope --mode-b dag-genesis --transport udp --runs 10
python -m trax_transport_lab.tcp_demo --json
python -m trax_transport_lab.udp_demo --json
python -m trax_transport_lab.udp_demo --json --include-events
python .\scripts\compare_transports.py --json --runs 10
python .\scripts\compare_transports.py --json --runs 5 --include-events
```

Metrics use `time.perf_counter_ns()` and are intended for local loopback comparison and regression tracking, not benchmark-grade performance claims.

Default JSON is compact. It includes wall-clock summaries, event-sum summaries, primitive highlights, counts, slowest events, final tip, and key event totals. It does not include raw `events` or `events_by_category`.

Use `--include-events` when you need raw event dumps:

```powershell
python -m trax_transport_lab.udp_demo --json --include-events
python .\scripts\compare_transports.py --json --runs 5 --include-events
```

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

Slowest events help identify major observed costs without dumping every raw event. Primitive highlights separate hash binding, DAG append, envelope creation, and envelope verification:

- `payload_hash_verify_us`
- `dag_append_event_us`
- `trax_hash32_event_us`
- `trax_create_envelope_event_us`
- `trax_create_envelope_event_ms`
- `trax_verify_envelope_event_us`
- `trax_verify_envelope_event_ms`

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

If `payload_hash_verify_us` is around 10-20 microseconds while admission envelope create/verify totals are tens of milliseconds, then payload binding is lightweight while direct Ed25519 admission envelope work dominates TRAX primitive event-sum time in this Python loopback harness.

These are local diagnostic measurements, not benchmark-grade claims.

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
