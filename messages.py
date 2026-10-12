"""Shared JSON messages and the binary header used by every inference stage."""
from dataclasses import asdict, dataclass
import json
import math
import socket
import struct


def _number(value):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise ValueError("Expected a finite, nonnegative number")


def _job(value):
    if type(value) is not int or value < 0:
        raise ValueError("Expected a nonnegative integer job index")


@dataclass
class AssignmentMessage:
    assignment: dict

    def __post_init__(self):
        if not isinstance(self.assignment, dict) or not self.assignment:
            raise ValueError("Assignment must be a nonempty mapping")
        for key, ip in self.assignment.items():
            if not isinstance(key, str) or not key.isdecimal():
                raise ValueError("Assignment keys must be string stage indices")
            socket.inet_pton(socket.AF_INET, ip)

    def to_bytes(self):
        return json.dumps(asdict(self), allow_nan=False).encode()

    @classmethod
    def from_bytes(cls, raw):
        return cls(**json.loads(raw))


@dataclass
class E2EMessage:
    request_id: int
    event: str
    timestamp: float

    def __post_init__(self):
        _job(self.request_id)
        if self.request_id >= 2**64 or self.event not in ("send", "receive"):
            raise ValueError("Invalid request ID or E2E event")
        _number(self.timestamp)

    def to_bytes(self):
        return json.dumps(asdict(self), allow_nan=False).encode()

    @classmethod
    def from_bytes(cls, raw):
        return cls(**json.loads(raw))


@dataclass
class PredictionTimeMessage:
    job: int  # Model stage, NOT an inference request ID.
    ip: str
    prediction_time: float

    def __post_init__(self):
        _job(self.job)
        socket.inet_pton(socket.AF_INET, self.ip)
        _number(self.prediction_time)

    def to_bytes(self):
        return json.dumps(asdict(self), allow_nan=False).encode()

    @classmethod
    def from_bytes(cls, raw):
        return cls(**json.loads(raw))


@dataclass
class TransferDelayMessage:
    job: int
    ip_i: str
    ip_j: str
    transfer_time: float

    def __post_init__(self):
        _job(self.job)
        socket.inet_pton(socket.AF_INET, self.ip_i)
        socket.inet_pton(socket.AF_INET, self.ip_j)
        _number(self.transfer_time)

    def to_bytes(self):
        return json.dumps(asdict(self), allow_nan=False).encode()

    @classmethod
    def from_bytes(cls, raw):
        return cls(**json.loads(raw))


# Network byte order: hop timestamp (8), original IPv4 (4), request ID (8).
# Payload: uint8 at the first stage, little-endian float32 after inference.
HEADER = struct.Struct("!d4sQ")


def pack_packet(payload, source_ip, request_id, timestamp):
    return HEADER.pack(timestamp, socket.inet_aton(source_ip), request_id) + payload


def unpack_packet(packet):
    if len(packet) < HEADER.size:
        raise ValueError("Truncated inference header")
    timestamp, source_ip, request_id = HEADER.unpack_from(packet)
    return timestamp, socket.inet_ntoa(source_ip), request_id, packet[HEADER.size:]


def recv_until_eof(sock, limit=1024 * 1024):
    """One control message per TCP connection; the sender half-closes its write side."""
    data = bytearray()
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            return bytes(data)
        data.extend(chunk)
        if len(data) > limit:
            raise ValueError("Control message too large")
