"""Minimal binary-safe RESP2/RESP3 client (stdlib only).

Deliberately independent of any Redis client library so the test suite
exercises the raw protocol, including replies that client libraries
normalize away (error classes, verbatim types, RESP3 frames).
"""

import socket


class ReplyError(Exception):
    """Server error reply (the message keeps the full error text)."""


class Client:
    def __init__(self, host="127.0.0.1", port=6379, timeout=30.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b""

    def close(self):
        self.sock.close()

    # -- protocol encoding ------------------------------------------------
    @staticmethod
    def _encode(args):
        out = [b"*%d\r\n" % len(args)]
        for a in args:
            if isinstance(a, str):
                a = a.encode()
            elif isinstance(a, int):
                a = str(a).encode()
            out.append(b"$%d\r\n%s\r\n" % (len(a), a))
        return b"".join(out)

    # -- protocol decoding ------------------------------------------------
    def _read_line(self):
        while b"\r\n" not in self.buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("server closed connection")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _read_exact(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("server closed connection")
            self.buf += chunk
        data, self.buf = self.buf[:n], self.buf[n:]
        return data

    def _read_reply(self):
        line = self._read_line()
        kind, rest = line[:1], line[1:]
        if kind == b"+":
            return rest.decode()
        if kind == b"-":
            raise ReplyError(rest.decode(errors="replace"))
        if kind == b":":
            return int(rest)
        if kind == b"$":                       # bulk string (bytes)
            n = int(rest)
            if n == -1:
                return None
            data = self._read_exact(n)
            self._read_exact(2)
            return data
        if kind == b"*" or kind == b">":       # array / push
            n = int(rest)
            if n == -1:
                return None
            return [self._read_reply() for _ in range(n)]
        # RESP3 frames
        if kind == b"_":                       # null
            return None
        if kind == b"#":                       # boolean
            return rest == b"t"
        if kind == b",":                       # double
            return float(rest)
        if kind == b"(":                       # big number
            return int(rest)
        if kind == b"=":                       # verbatim string
            n = int(rest)
            data = self._read_exact(n)
            self._read_exact(2)
            return data[4:]                    # strip "txt:" prefix
        if kind == b"%":                       # map
            n = int(rest)
            return {self._as_key(self._read_reply()): self._read_reply() for _ in range(n)}
        if kind == b"~":                       # set
            n = int(rest)
            return [self._read_reply() for _ in range(n)]
        raise ValueError(f"unknown RESP type byte: {kind!r}")

    @staticmethod
    def _as_key(k):
        return k.decode() if isinstance(k, bytes) else k

    # -- public API -------------------------------------------------------
    def cmd(self, *args):
        """Send one command, return its reply (raises ReplyError on -ERR)."""
        self.sock.sendall(self._encode(args))
        return self._read_reply()

    def cmd_err(self, *args):
        """Send one command that must fail; return the error text."""
        try:
            reply = self.cmd(*args)
        except ReplyError as e:
            return str(e)
        raise AssertionError(f"expected error for {args!r}, got {reply!r}")

    def pipeline(self, commands):
        """Send many commands in one write; return list of replies.

        Error replies come back as ReplyError instances (not raised).
        """
        self.sock.sendall(b"".join(self._encode(c) for c in commands))
        replies = []
        for _ in commands:
            try:
                replies.append(self._read_reply())
            except ReplyError as e:
                replies.append(e)
        return replies
