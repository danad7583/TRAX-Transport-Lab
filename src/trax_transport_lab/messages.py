import hashlib
import json
from typing import Any


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
}


class MessageError(ValueError):
    """Raised when a demo JSON message is invalid."""


def canonical_json(obj: dict[str, Any]) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")


def bytes_to_hex(value: bytes) -> str:
    return value.hex()


def hex_to_bytes(value: str, field_name: str) -> bytes:
    if not isinstance(value, str):
        raise MessageError(f"{field_name} must be a hex string")
    try:
        return bytes.fromhex(value)
    except ValueError as exc:
        raise MessageError(f"{field_name} must be valid hex") from exc


def encode_message(message: dict[str, Any]) -> bytes:
    normalized = {
        "demo_protocol": DEMO_PROTOCOL,
        "demo_version": DEMO_VERSION,
        **message,
    }
    validate_message(normalized)
    return canonical_json(normalized)


def decode_message(data: bytes) -> dict[str, Any]:
    try:
        message = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MessageError("message must be valid UTF-8 JSON") from exc
    if not isinstance(message, dict):
        raise MessageError("message must be a JSON object")
    validate_message(message)
    return message


def hash_message(message: dict[str, Any]) -> bytes:
    return hashlib.blake2s(encode_message(message), digest_size=32).digest()


def validate_message(message: dict[str, Any]) -> None:
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
            hex_to_bytes(message[field], field)

    if "dag_parent_refs" in message:
        refs = message["dag_parent_refs"]
        if not isinstance(refs, list):
            raise MessageError("dag_parent_refs must be a list")
        for ref in refs:
            hex_to_bytes(ref, "dag_parent_refs[]")

    if "cycle_index" in message:
        cycle_index = message["cycle_index"]
        if not isinstance(cycle_index, int) or cycle_index < 0:
            raise MessageError("cycle_index must be a non-negative integer")
