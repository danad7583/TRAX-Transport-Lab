import socket
import struct


MAX_FRAME_LEN = 1024 * 1024
MAX_PACKET_LEN = 64 * 1024


class FramingError(Exception):
    """Base class for TCP demo framing errors."""


class OversizedFrameError(FramingError):
    """Raised when a frame declares or sends too many bytes."""


class TruncatedFrameError(FramingError):
    """Raised when the socket closes before the complete frame arrives."""


class EmptyFrameError(FramingError):
    """Raised when a zero-length frame is received."""


def send_frame(sock: socket.socket, data: bytes) -> None:
    if not isinstance(data, bytes):
        raise TypeError("frame data must be bytes")
    if len(data) == 0:
        raise EmptyFrameError("zero-length frames are not allowed")
    if len(data) > MAX_FRAME_LEN:
        raise OversizedFrameError(f"frame length {len(data)} exceeds {MAX_FRAME_LEN}")

    sock.sendall(struct.pack(">I", len(data)) + data)


def recv_exact(sock: socket.socket, n: int) -> bytes:
    chunks: list[bytes] = []
    remaining = n
    while remaining:
        try:
            chunk = sock.recv(remaining)
        except socket.timeout as exc:
            raise TruncatedFrameError(f"timed out while reading {n} bytes") from exc
        if not chunk:
            raise TruncatedFrameError(f"socket closed with {remaining} bytes remaining")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def recv_frame(sock: socket.socket, max_len: int = MAX_FRAME_LEN) -> bytes:
    header = recv_exact(sock, 4)
    (frame_len,) = struct.unpack(">I", header)
    if frame_len == 0:
        raise EmptyFrameError("zero-length frames are not allowed")
    if frame_len > max_len:
        raise OversizedFrameError(f"frame length {frame_len} exceeds {max_len}")
    return recv_exact(sock, frame_len)
