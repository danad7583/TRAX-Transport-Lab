import hashlib
import importlib
from dataclasses import dataclass
from contextlib import nullcontext
from typing import Any

from .metrics import CATEGORY_TRAX, RunMetrics


class TraxAdapterError(RuntimeError):
    """Raised when the installed TRAX module cannot satisfy the lab API."""


@dataclass(frozen=True)
class KeyPair:
    private_key: Any
    public_key: bytes


class TraxAdapter:
    def __init__(self, module: Any | None = None, metrics: RunMetrics | None = None):
        self.trax = module if module is not None else importlib.import_module("trax")
        self.metrics = metrics
        self._validate_api()

    def _measure(self, name: str):
        if self.metrics is None:
            return nullcontext()
        return self.metrics.measure(name, CATEGORY_TRAX)

    def _validate_api(self) -> None:
        required = [
            "generate_keypair",
            "generate_nonce",
            "hash32",
            "derive_session_id",
            "create_admission_envelope_v1",
            "verify_admission_envelope_v1_for_receiver",
            "decode_admission_envelope_v1",
            "LocalDag",
        ]
        missing = [name for name in required if not hasattr(self.trax, name)]
        if missing:
            raise TraxAdapterError(f"trax module missing: {', '.join(missing)}")

    def generate_keypair(self) -> KeyPair:
        with self._measure("trax.generate_keypair"):
            keypair = self.trax.generate_keypair()
        private_key = keypair["private_key"]
        public_key = bytes(keypair["public_key"])
        if len(public_key) != 32:
            raise TraxAdapterError("TRAX public key must be 32 bytes")
        return KeyPair(private_key=private_key, public_key=public_key)

    def generate_nonce(self) -> bytes:
        with self._measure("trax.generate_nonce"):
            nonce = bytes(self.trax.generate_nonce())
        if len(nonce) != 16:
            raise TraxAdapterError("TRAX nonce must be 16 bytes")
        return nonce

    def hash32(self, data: bytes) -> bytes:
        with self._measure("trax.hash32"):
            digest = bytes(self.trax.hash32(data))
        if len(digest) != 32:
            raise TraxAdapterError("TRAX hash32 must return 32 bytes")
        return digest

    def derive_session_id(
        self,
        transcript_hash: bytes,
        client_nonce: bytes,
        server_nonce: bytes,
    ) -> bytes:
        with self._measure("trax.derive_session_id"):
            session_id = bytes(
                self.trax.derive_session_id(transcript_hash, client_nonce, server_nonce)
            )
        if len(session_id) != 32:
            raise TraxAdapterError("TRAX session_id must be 32 bytes")
        return session_id

    def create_envelope(
        self,
        private_key,
        receiver_public_key: bytes,
        session_id: bytes,
        payload: bytes,
        message_type: str,
        dag_parent_refs: list[bytes] | None = None,
        proof_type: str = "direct-ed25519",
    ) -> bytes:
        nonce = self.generate_nonce()
        try:
            with self._measure("trax.create_admission_envelope_v1"):
                return bytes(
                    self.trax.create_admission_envelope_v1(
                        private_key,
                        receiver_public_key,
                        session_id,
                        nonce,
                        payload,
                        message_type,
                        dag_parent_refs or [],
                        proof_type,
                    )
                )
        except ValueError as exc:
            if proof_type != "none":
                with self._measure("trax.create_admission_envelope_v1"):
                    return bytes(
                        self.trax.create_admission_envelope_v1(
                            private_key,
                            receiver_public_key,
                            session_id,
                            nonce,
                            payload,
                            message_type,
                            dag_parent_refs or [],
                            "none",
                        )
                    )
            raise TraxAdapterError(str(exc)) from exc

    def verify_for_receiver(
        self,
        envelope: bytes,
        payload: bytes,
        receiver_public_key: bytes,
    ) -> bool:
        try:
            with self._measure("trax.verify_admission_envelope_v1_for_receiver"):
                return bool(
                    self.trax.verify_admission_envelope_v1_for_receiver(
                        envelope, payload, receiver_public_key
                    )
                )
        except ValueError:
            return False

    def decode_envelope(self, envelope: bytes) -> dict[str, Any]:
        with self._measure("trax.decode_admission_envelope_v1"):
            return dict(self.trax.decode_admission_envelope_v1(envelope))


class DevelopmentFallbackTraxAdapter(TraxAdapter):
    """Explicit fallback for local design work when TRAX bindings are not installed.

    Tests intentionally import the real ``trax`` module and should not rely on this.
    It exists only so non-security message/DAG code can be exercised during early
    development on machines before ``maturin develop`` has run.
    """

    def __init__(self):
        self.trax = None
        self.metrics = None

    def _validate_api(self) -> None:
        return None

    def generate_keypair(self) -> KeyPair:
        seed = hashlib.blake2s(str(id(self)).encode(), digest_size=32).digest()
        return KeyPair(private_key=seed, public_key=self.hash32(b"public" + seed))

    def generate_nonce(self) -> bytes:
        return hashlib.blake2s(str(id(object())).encode(), digest_size=16).digest()

    def hash32(self, data: bytes) -> bytes:
        return hashlib.blake2s(data, digest_size=32).digest()

    def derive_session_id(
        self,
        transcript_hash: bytes,
        client_nonce: bytes,
        server_nonce: bytes,
    ) -> bytes:
        return self.hash32(transcript_hash + client_nonce + server_nonce)

    def create_envelope(
        self,
        private_key,
        receiver_public_key: bytes,
        session_id: bytes,
        payload: bytes,
        message_type: str,
        dag_parent_refs: list[bytes] | None = None,
        proof_type: str = "direct-ed25519",
    ) -> bytes:
        body = b"|".join(
            [
                bytes(private_key),
                receiver_public_key,
                session_id,
                self.hash32(payload),
                message_type.encode(),
                b"".join(dag_parent_refs or []),
            ]
        )
        return self.hash32(body) + self.hash32(b"fallback" + body)

    def verify_for_receiver(
        self,
        envelope: bytes,
        payload: bytes,
        receiver_public_key: bytes,
    ) -> bool:
        return len(envelope) == 64 and len(receiver_public_key) == 32 and bool(payload)

    def decode_envelope(self, envelope: bytes) -> dict[str, Any]:
        return {"fallback": True, "envelope_hash": self.hash32(envelope)}
