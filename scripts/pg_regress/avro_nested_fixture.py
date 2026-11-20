"""Regenerate lagodb-connectors/tests/pg_regress/data/avro_nested.avro.

The container is written by this standalone encoder rather than by LagoDB, so
the regression reads a file its own writer cannot produce: nested records,
arrays and maps (including byte-sized negative-count blocks), an enum, fixed
values, named and recursive references, a uuid over fixed(16), and a union
whose non-null branch comes first. avro.sql projects id, label, ref_id, amount
and flag, so every other field must be skipped; parent_id skips the same
fixed(16) uuid through a named reference.

The output is deterministic (null codec, fixed sync marker); run this script
and commit the result whenever the rows below change.
"""

import json
import struct
from uuid import UUID
from pathlib import Path

OUTPUT = (
    Path(__file__).resolve().parents[2]
    / "lagodb-connectors/tests/pg_regress/data/avro_nested.avro"
)
SYNC_MARKER = b"lagodb-avro-sync"

SCHEMA = {
    "type": "record",
    "name": "Event",
    "namespace": "lagodb.regress",
    "fields": [
        {"name": "id", "type": "int"},
        {
            "name": "origin",
            "type": {
                "type": "record",
                "name": "Point",
                "fields": [
                    {"name": "x", "type": "double"},
                    {"name": "tag", "type": ["string", "null"]},
                ],
            },
        },
        {"name": "label", "type": "string"},
        {
            "name": "ref_id",
            "type": {
                "type": "fixed",
                "name": "RefId",
                "size": 16,
                "logicalType": "uuid",
            },
        },
        {"name": "samples", "type": {"type": "array", "items": "Point"}},
        {
            "name": "attributes",
            "type": {"type": "map", "values": {"type": "array", "items": "long"}},
        },
        {"name": "amount", "type": ["long", "null"]},
        {
            "name": "state",
            "type": {"type": "enum", "name": "State", "symbols": ["NEW", "DONE"]},
        },
        {"name": "digest", "type": {"type": "fixed", "name": "Digest", "size": 4}},
        {"name": "previous", "type": ["null", "Digest"]},
        {"name": "parent_id", "type": ["null", "RefId"]},
        {
            "name": "chain",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "Link",
                    "fields": [
                        {"name": "value", "type": "int"},
                        {"name": "next", "type": ["null", "Link"]},
                    ],
                },
            ],
        },
        {"name": "flag", "type": "boolean"},
    ],
}


def long(value: int) -> bytes:
    encoded = (value << 1) ^ (value >> 63)
    out = bytearray()
    while encoded & ~0x7F:
        out.append((encoded & 0x7F) | 0x80)
        encoded >>= 7
    out.append(encoded)
    return bytes(out)


def sized(value: bytes) -> bytes:
    return long(len(value)) + value


def string(value: str) -> bytes:
    return sized(value.encode())


def double(value: float) -> bytes:
    return struct.pack("<d", value)


def boolean(value: bool) -> bytes:
    return b"\x01" if value else b"\x00"


def blocks(items: list[bytes], sizes: list[int], *, byte_sized: bool) -> bytes:
    """Encode array or map items in blocks of `sizes` items each.

    A byte-sized block uses a negative count followed by its byte size.
    """
    out = bytearray()
    start = 0
    for size in sizes:
        payload = b"".join(items[start : start + size])
        start += size
        if byte_sized:
            out += long(-size) + long(len(payload)) + payload
        else:
            out += long(size) + payload
    assert start == len(items)
    return bytes(out + long(0))


def uuid(value: str) -> bytes:
    """A uuid over fixed(16): the 16 raw bytes, with no length prefix."""
    return UUID(value).bytes


def point(x: float, tag: str | None) -> bytes:
    return double(x) + (long(0) + string(tag) if tag is not None else long(1))


def link(values: list[int]) -> bytes:
    """A Link chain wrapped in its ["null", Link] union."""
    out = bytearray()
    for value in values:
        out += long(1) + long(value)
    return bytes(out + long(0))


def row(
    *,
    id: int,
    origin: bytes,
    label: str,
    ref_id: str,
    samples: bytes,
    attributes: bytes,
    amount: int | None,
    state: int,
    digest: bytes,
    previous: bytes | None,
    parent_id: str | None,
    chain: list[int],
    flag: bool,
) -> bytes:
    assert len(digest) == 4
    return b"".join(
        [
            long(id),
            origin,
            string(label),
            uuid(ref_id),
            samples,
            attributes,
            long(0) + long(amount) if amount is not None else long(1),
            long(state),
            digest,
            long(1) + previous if previous is not None else long(0),
            long(1) + uuid(parent_id) if parent_id is not None else long(0),
            link(chain),
            boolean(flag),
        ]
    )


def entry(key: str, values: list[int], sizes: list[int]) -> bytes:
    return string(key) + blocks(
        [long(value) for value in values], sizes, byte_sized=False
    )


ROWS = [
    row(
        id=1,
        origin=point(1.5, "a"),
        label="first",
        ref_id="00112233-4455-6677-8899-aabbccddeeff",
        samples=blocks([point(2.0, None)], [1], byte_sized=False),
        attributes=blocks([entry("k", [1, 2], [2])], [1], byte_sized=True),
        amount=10,
        state=1,
        digest=b"\x00\x01\x02\x03",
        previous=None,
        parent_id=None,
        chain=[1, 2],
        flag=True,
    ),
    row(
        id=2,
        origin=point(-3.0, None),
        label="second",
        ref_id="f47ac10b-58cc-4372-a567-0e02b2c3d479",
        samples=blocks(
            [point(4.0, "b"), point(5.0, None)], [2], byte_sized=True
        ),
        attributes=blocks([], [], byte_sized=False),
        amount=None,
        state=0,
        digest=b"\xff\xfe\xfd\xfc",
        previous=b"\x00\x01\x02\x03",
        parent_id="00112233-4455-6677-8899-aabbccddeeff",
        chain=[],
        flag=False,
    ),
    row(
        id=3,
        origin=point(0.0, "c"),
        label="third",
        ref_id="a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
        samples=blocks(
            [point(6.0, "d"), point(7.0, "e"), point(8.0, None)],
            [1, 2],
            byte_sized=False,
        ),
        attributes=blocks(
            [entry("x", [3], [1]), entry("y", [4, 5, 6], [2, 1])],
            [2],
            byte_sized=False,
        ),
        amount=-7,
        state=1,
        digest=b"\x10\x20\x30\x40",
        previous=b"\xff\xfe\xfd\xfc",
        parent_id="f47ac10b-58cc-4372-a567-0e02b2c3d479",
        chain=[3],
        flag=True,
    ),
]

# Rows 1-2 share a block, so a block boundary falls between rows 2 and 3.
BLOCKS = [ROWS[0:2], ROWS[2:3]]


def container() -> bytes:
    schema = json.dumps(SCHEMA, separators=(",", ":")).encode()
    metadata = (
        long(2)
        + string("avro.schema")
        + sized(schema)
        + string("avro.codec")
        + sized(b"null")
        + long(0)
    )
    out = bytearray(b"Obj\x01" + metadata + SYNC_MARKER)
    for block in BLOCKS:
        payload = b"".join(block)
        out += long(len(block)) + long(len(payload)) + payload + SYNC_MARKER
    return bytes(out)


if __name__ == "__main__":
    OUTPUT.write_bytes(container())
