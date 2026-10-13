"""Stdlib-only AES-128 (ECB, PKCS7) for the CBA data API's response encryption.

Every ``data-server.cbaleague.com`` response is a JSON string holding base64 of the AES-128-ECB / PKCS7 ciphertext of the
real JSON body (the site's bundle decrypts it with CryptoJS in function ``TIe``). Decrypting needs no shell-out and no
third-party package: this module is a plain FIPS-197 implementation (tables built at import), so the repo's full-suite
command (which has no ``cryptography`` / ``pycryptodome``) can run the capture tests. ``encrypt_*`` exists for the tests
and for fixtures. ECB has no integrity check: a wrong key almost always fails the PKCS7 / UTF-8 / JSON checks below, and a
body that fails any of them is refused (never banked as plain text).

    ponytail: pure-Python AES is ~100 KB/s; bodies here are < 100 KB. Swap in `cryptography` if a route ever grows.
"""

from __future__ import annotations

import base64
import binascii
import json


class Undecryptable(ValueError):
    """The body is not a valid base64 / AES-128-ECB / PKCS7 / UTF-8 / JSON envelope under the given key."""


def _xtime(a: int) -> int:
    a <<= 1
    return (a ^ 0x11B) & 0xFF if a & 0x100 else a


def _build_tables():
    # GF(2^8) log/antilog over generator 3, then the S-box from the multiplicative inverse + affine map.
    exp, log = [0] * 256, [0] * 256
    x = 1
    for i in range(255):
        exp[i], log[x] = x, i
        x ^= _xtime(x)  # x * 3
    sbox = [0] * 256
    for v in range(256):
        inv = 0 if v == 0 else exp[(255 - log[v]) % 255]
        s = inv
        for _ in range(4):
            inv = ((inv << 1) | (inv >> 7)) & 0xFF
            s ^= inv
        sbox[v] = s ^ 0x63
    inv_sbox = [0] * 256
    for i, v in enumerate(sbox):
        inv_sbox[v] = i
    return sbox, inv_sbox, exp, log


SBOX, INV_SBOX, _EXP, _LOG = _build_tables()


def _mul(a: int, b: int) -> int:
    return 0 if a == 0 or b == 0 else _EXP[(_LOG[a] + _LOG[b]) % 255]


def _expand_key(key: bytes) -> list[list[int]]:
    if len(key) != 16:
        raise Undecryptable(f"AES-128 key must be 16 bytes, got {len(key)}")
    w = [list(key[4 * i : 4 * i + 4]) for i in range(4)]
    rcon = 1
    for i in range(4, 44):
        t = list(w[i - 1])
        if i % 4 == 0:
            t = [SBOX[b] for b in t[1:] + t[:1]]
            t[0] ^= rcon
            rcon = _xtime(rcon)
        w.append([a ^ b for a, b in zip(w[i - 4], t)])
    return [sum(w[4 * r : 4 * r + 4], []) for r in range(11)]  # 11 round keys of 16 bytes


def _add(state: list[int], rk: list[int]) -> list[int]:
    return [a ^ b for a, b in zip(state, rk)]


# state is column-major (byte i = row i % 4, column i // 4), the same order as the input block
_SHIFT = [0, 5, 10, 15, 4, 9, 14, 3, 8, 13, 2, 7, 12, 1, 6, 11]
_UNSHIFT = [0, 13, 10, 7, 4, 1, 14, 11, 8, 5, 2, 15, 12, 9, 6, 3]


def _mix(state: list[int], m: tuple[int, int, int, int]) -> list[int]:
    out = []
    for c in range(4):
        col = state[4 * c : 4 * c + 4]
        for r in range(4):
            out.append(
                _mul(m[0], col[r])
                ^ _mul(m[1], col[(r + 1) % 4])
                ^ _mul(m[2], col[(r + 2) % 4])
                ^ _mul(m[3], col[(r + 3) % 4])
            )
    return out


def decrypt_block(block: bytes, rks: list[list[int]]) -> bytes:
    s = _add(list(block), rks[10])
    for r in range(9, -1, -1):
        s = [INV_SBOX[s[i]] for i in _UNSHIFT]
        s = _add(s, rks[r])
        if r:
            s = _mix(s, (14, 11, 13, 9))
    return bytes(s)


def encrypt_block(block: bytes, rks: list[list[int]]) -> bytes:
    s = _add(list(block), rks[0])
    for r in range(1, 11):
        s = [SBOX[b] for b in s]
        s = [s[i] for i in _SHIFT]
        if r < 10:
            s = _mix(s, (2, 3, 1, 1))
        s = _add(s, rks[r])
    return bytes(s)


def encrypt_ecb(plain: bytes, key: bytes) -> bytes:
    """PKCS7-pad and AES-128-ECB encrypt (tests / fixtures only)."""
    rks = _expand_key(key)
    pad = 16 - len(plain) % 16
    data = plain + bytes([pad]) * pad
    return b"".join(encrypt_block(data[i : i + 16], rks) for i in range(0, len(data), 16))


def decrypt_ecb(cipher: bytes, key: bytes) -> bytes:
    """AES-128-ECB decrypt and strictly remove the PKCS7 padding (every pad byte checked)."""
    if not cipher or len(cipher) % 16:
        raise Undecryptable(f"ciphertext length {len(cipher)} is not a positive multiple of 16")
    rks = _expand_key(key)
    plain = b"".join(decrypt_block(cipher[i : i + 16], rks) for i in range(0, len(cipher), 16))
    pad = plain[-1]
    if not 1 <= pad <= 16 or plain[-pad:] != bytes([pad]) * pad:
        raise Undecryptable("bad PKCS7 padding (wrong key?)")
    return plain[:-pad]


def encrypt_body(obj, key: bytes) -> str:
    """The wire form of a response: a JSON string literal holding base64 of the ciphertext (tests / fixtures only)."""
    raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return json.dumps(base64.b64encode(encrypt_ecb(raw, key)).decode("ascii"))


def decrypt_body(text: str, key: bytes):
    """Wire text -> parsed JSON. Accepts the JSON-string-literal form the API sends (and bare base64). Raises Undecryptable
    for anything else: a plain-JSON body (the API's 500 envelope is not encrypted), non-base64 text, a wrong key, a
    non-UTF-8 or non-JSON plaintext."""
    t = (text or "").strip()
    if t.startswith('"'):
        try:
            t = json.loads(t)
        except ValueError:
            raise Undecryptable("body starts with a quote but is not a JSON string") from None
    if not isinstance(t, str) or not t:
        raise Undecryptable("body is not a base64 string")
    try:
        cipher = base64.b64decode(t, validate=True)
    except (binascii.Error, ValueError):
        raise Undecryptable("body is not base64 (plain / error body?)") from None
    plain = decrypt_ecb(cipher, key)
    try:
        return json.loads(plain.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        raise Undecryptable("plaintext is not UTF-8 JSON (wrong key?)") from None
