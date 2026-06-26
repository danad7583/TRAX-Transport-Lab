import hashlib
import json
from typing import Any

from .metrics import CATEGORY_PYTHON_PACKAGING, RunMetrics


DEMO_PROTOCOL = "trax-transport-lab"
DEMO_VERSION = 1

MESSAGE_TYPES = {
    "TRAX_INIT",
    "TRAX_INIT_ACK",
    "TRAX_COMMIT",
    "TRAX_REQ",
    "TRAX_REQ_ACK",
    "JUNK_STREAM_PAYLOAD",
    "TRAX_RES_ACK",
    "TRAX_CHECKPOINT",
    "TRAX_GENESIS",
}

HEX_FIELDS = {
    "session_id",
    "client_nonce",
    "server_nonce",
    "sender_public_key",
    "receiver_public_key",
    "payload_hash",
    "admission_envelope",
    "payload",
    "previous_tip",
    "event_hash",
    "checkpoint_content_hash",
    "aggregate_hash",
    "genesis_tip",
    "genesis_hash",
}

REQUIRED_FIELDS = {
    "TRAX_INIT": {
        "message_type",
        "client_nonce",
        "sender_public_key",
        "receiver_public_key",
        "admission_envelope",
        "payload_hash",
    },
    "TRAX_INIT_ACK": {
        "message_type",
        "session_id",
        "client_nonce",
        "server_nonce",
        "sender_public_key",
        "receiver_public_key",
        "admission_envelope",
        "payload_hash",
    },
    "TRAX_COMMIT": {
        "message_type",
        "session_id",
        "sender_public_key",
        "receiver_public_key",
        "admission_envelope",
        "payload_hash",
    },
    "TRAX_REQ": {
        "message_type",
        "session_id",
        "sender_public_key",
        "receiver_public_key",
        "payload_hash",
        "admission_envelope",
        "cycle_index",
    },
    "TRAX_REQ_ACK": {
        "message_type",
        "session_id",
        "sender_public_key",
        "receiver_public_key",
        "payload_hash",
        "admission_envelope",
        "cycle_index",
    },
    "JUNK_STREAM_PAYLOAD": {
        "message_type",
        "session_id",
        "payload",
        "payload_hash",
        "cycle_index",
    },
    "TRAX_RES_ACK": {
        "message_type",
        "session_id",
        "sender_public_key",
        "receiver_public_key",
        "payload_hash",
        "admission_envelope",
        "cycle_index",
    },
    "TRAX_CHECKPOINT": {
        "message_type",
        "session_id",
        "sender_public_key",
        "receiver_public_key",
        "payload_hash",
        "admission_envelope",
        "checkpoint_content_hash",
    },
    "TRAX_GENESIS": {
        "message_type",
        "session_id",
        "sender_public_key",
        "receiver_public_key",
        "payload_hash",
        "admission_envelope",
        "genesis_hash",
    },
}


class MessageError(ValueError):
    """Raised when a demo JSON message is invalid."""


def canonical_json(obj: dict[str, Any], metrics: RunMetrics | None = None) -> bytes:
    if metrics is None:
        return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    with metrics.measure("json.dumps", CATEGORY_PYTHON_PACKAGING):
        return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")


def bytes_to_hex(value: bytes, metrics: RunMetrics | None = None) -> str:
    if metrics is None:
        return value.hex()
    with metrics.measure("bytes.hex", CATEGORY_PYTHON_PACKAGING):
        return value.hex()


def hex_to_bytes(
    value: str,
    field_name: str,
    metrics: RunMetrics | None = None,
) -> bytes:
    if not isinstance(value, str):
        raise MessageError(f"{field_name} must be a hex string")
    try:
        if metrics is None:
            return bytes.fromhex(value)
        with metrics.measure("bytes.fromhex", CATEGORY_PYTHON_PACKAGING):
            return bytes.fromhex(value)
    except ValueError as exc:
        raise MessageError(f"{field_name} must be valid hex") from exc


def encode_message(message: dict[str, Any], metrics: RunMetrics | None = None) -> bytes:
    def _encode() -> bytes:
        normalized = {
            "demo_protocol": DEMO_PROTOCOL,
            "demo_version": DEMO_VERSION,
            **message,
        }
        validate_message(normalized, metrics=metrics)
        return canonical_json(normalized, metrics=metrics)

    if metrics is None:
        return _encode()
    with metrics.measure("message.encode", CATEGORY_PYTHON_PACKAGING):
        return _encode()


def decode_message(data: bytes, metrics: RunMetrics | None = None) -> dict[str, Any]:
    def _decode() -> dict[str, Any]:
        try:
            if metrics is None:
                message = json.loads(data.decode("utf-8"))
            else:
                with metrics.measure("json.loads", CATEGORY_PYTHON_PACKAGING):
                    message = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MessageError("message must be valid UTF-8 JSON") from exc
        if not isinstance(message, dict):
            raise MessageError("message must be a JSON object")
        validate_message(message, metrics=metrics)
        return message

    if metrics is None:
        return _decode()
    with metrics.measure("message.decode", CATEGORY_PYTHON_PACKAGING):
        return _decode()


def hash_message(message: dict[str, Any], metrics: RunMetrics | None = None) -> bytes:
    def _hash() -> bytes:
        return hashlib.blake2s(encode_message(message, metrics=metrics), digest_size=32).digest()

    if metrics is None:
        return _hash()
    with metrics.measure("message.hash", CATEGORY_PYTHON_PACKAGING):
        return _hash()


def validate_message(message: dict[str, Any], metrics: RunMetrics | None = None) -> None:
    if message.get("demo_protocol") != DEMO_PROTOCOL:
        raise MessageError("invalid demo_protocol")
    if message.get("demo_version") != DEMO_VERSION:
        raise MessageError("invalid demo_version")

    message_type = message.get("message_type")
    if message_type not in MESSAGE_TYPES:
        raise MessageError(f"unsupported message_type={message_type!r}")

    required = REQUIRED_FIELDS[message_type]
    missing = sorted(field for field in required if field not in message)
    if missing:
        raise MessageError(f"missing required fields: {', '.join(missing)}")

    for field in HEX_FIELDS:
        if field in message:
            hex_to_bytes(message[field], field, metrics=metrics)

    if "dag_parent_refs" in message:
        refs = message["dag_parent_refs"]
        if not isinstance(refs, list):
            raise MessageError("dag_parent_refs must be a list")
        for ref in refs:
            hex_to_bytes(ref, "dag_parent_refs[]", metrics=metrics)

    if "cycle_index" in message:
        cycle_index = message["cycle_index"]
        if not isinstance(cycle_index, int) or cycle_index < 0:
            raise MessageError("cycle_index must be a non-negative integer")

    if "counter" in message:
        counter = message["counter"]
        if not isinstance(counter, int) or counter < 0:
            raise MessageError("counter must be a non-negative integer")
