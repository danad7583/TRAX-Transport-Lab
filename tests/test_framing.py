import socket
import struct

import pytest

from trax_transport_lab.framing import (
    EmptyFrameError,
    MAX_FRAME_LEN,
    OversizedFrameError,
    TruncatedFrameError,
    recv_frame,
    send_frame,
)


def test_frame_round_trip():
    left, right = socket.socketpair()
    try:
        send_frame(left, b"abc123")
        assert recv_frame(right) == b"abc123"
    finally:
        left.close()
        right.close()


def test_oversized_frame_rejected():
    left, right = socket.socketpair()
    try:
        left.sendall(struct.pack(">I", MAX_FRAME_LEN + 1))
        with pytest.raises(OversizedFrameError):
            recv_frame(right)
    finally:
        left.close()
        right.close()


def test_zero_length_frame_rejected():
    left, right = socket.socketpair()
    try:
        left.sendall(struct.pack(">I", 0))
        with pytest.raises(EmptyFrameError):
            recv_frame(right)
    finally:
        left.close()
        right.close()


def test_truncated_frame_rejected():
    left, right = socket.socketpair()
    try:
        left.sendall(struct.pack(">I", 8) + b"abc")
        left.close()
        with pytest.raises(TruncatedFrameError):
            recv_frame(right)
    finally:
        right.close()
