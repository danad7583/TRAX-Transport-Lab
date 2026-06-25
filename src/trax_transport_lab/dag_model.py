from dataclasses import dataclass
import hashlib
import json


NODE_TYPES = {"SESSION_START_V0", "STREAM_EXCHANGE_V0"}


class DagError(ValueError):
    """Raised when the demo DAG cannot append a node."""


@dataclass(frozen=True)
class DemoDagNode:
    index: int
    node_type: str
    session_id: bytes
    parent_hashes: list[bytes]
    content_hash: bytes
    node_hash: bytes
    packet_hashes: dict[str, bytes]


def _hash_bytes(data: bytes) -> bytes:
    return hashlib.blake2s(data, digest_size=32).digest()


def _canonical_json(obj: dict) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _hex_list(values: list[bytes]) -> list[str]:
    return [value.hex() for value in values]


class DemoDag:
    def __init__(self):
        self._nodes: list[DemoDagNode] = []

    def append_node(
        self,
        node_type: str,
        session_id: bytes,
        parent_hashes: list[bytes],
        packet_hashes: dict[str, bytes],
    ) -> DemoDagNode:
        if node_type not in NODE_TYPES:
            raise DagError(f"unsupported node_type={node_type}")

        expected_parent = self.final_tip()
        if expected_parent is None:
            if parent_hashes:
                raise DagError("first node must not reference a parent tip")
        elif parent_hashes != [expected_parent]:
            raise DagError("node must reference the current final tip")

        index = len(self._nodes)
        content_obj = {
            "index": index,
            "node_type": node_type,
            "session_id": session_id.hex(),
            "parent_hashes": _hex_list(parent_hashes),
            "packet_hashes": {
                key: packet_hashes[key].hex() for key in sorted(packet_hashes)
            },
        }
        content_hash = _hash_bytes(_canonical_json(content_obj))
        node_obj = {
            **content_obj,
            "content_hash": content_hash.hex(),
        }
        node_hash = _hash_bytes(_canonical_json(node_obj))
        node = DemoDagNode(
            index=index,
            node_type=node_type,
            session_id=session_id,
            parent_hashes=list(parent_hashes),
            content_hash=content_hash,
            node_hash=node_hash,
            packet_hashes=dict(packet_hashes),
        )
        self._nodes.append(node)
        return node

    def final_tip(self) -> bytes | None:
        if not self._nodes:
            return None
        return self._nodes[-1].node_hash

    def enumerate(self) -> list[DemoDagNode]:
        return list(self._nodes)

    def __len__(self) -> int:
        return len(self._nodes)
