"""Raw RESP client: replies as the exact bytes the server wrote.

The parsed client (valkey_client.py) is right for most checks, but it maps
different wire types to the same Python value: a bulk string and a simple
string both become bytes, a RESP3 double and a bulk string can print the
same, a verbatim string loses its format tag. Reply parity with upstream is
a promise about bytes, so the strict differential rows read whole reply
frames instead.

Each command is followed by an ECHO marker and everything up to the marker
is its reply. That keeps the connection in step even when a server answers
one command with zero or several frames (upstream redis-roaring replies
WRONGTYPE twice for a variadic BITOP with a wrong-type source).
"""

import socket


def encode(args):
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        if isinstance(a, str):
            a = a.encode()
        elif isinstance(a, int):
            a = str(a).encode()
        out.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(out)


class RawClient:
    def __init__(self, host="127.0.0.1", port=6379, resp=2, timeout=60.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.buf = b""
        self.markers = 0
        if resp == 3:
            self.frames("HELLO", "3")

    def close(self):
        self.sock.close()

    def _fill(self):
        chunk = self.sock.recv(65536)
        if not chunk:
            raise ConnectionError("server closed the connection")
        self.buf += chunk

    def _line(self):
        while b"\r\n" not in self.buf:
            self._fill()
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line + b"\r\n"

    def _exact(self, n):
        while len(self.buf) < n:
            self._fill()
        data, self.buf = self.buf[:n], self.buf[n:]
        return data

    def _frame(self):
        line = self._line()
        kind = line[:1]
        if kind in (b"$", b"=", b"!"):
            n = int(line[1:-2])
            return line if n < 0 else line + self._exact(n + 2)
        if kind in (b"*", b"~", b">"):
            n = int(line[1:-2])
            return line + b"".join(self._frame() for _ in range(max(n, 0)))
        if kind == b"%":
            n = int(line[1:-2])
            return line + b"".join(self._frame() for _ in range(2 * n))
        return line

    def frames(self, *args):
        """Every reply frame `args` produced, in order."""
        self.markers += 1
        marker = b"__raw_resp_sync_%d__" % self.markers
        self.sock.sendall(encode(list(args)) + encode(["ECHO", marker]))
        out = []
        while True:
            frame = self._frame()
            if frame.endswith(b"\r\n" + marker + b"\r\n") and frame[:1] in (b"$", b"+"):
                return out
            out.append(frame)

    def reply(self, *args):
        """The reply bytes of one command. A command that replies more or
        less than once is rendered as `<n replies>` plus the frames, so it
        can never compare equal to a single well-formed reply."""
        out = self.frames(*args)
        if len(out) == 1:
            return out[0]
        return b"<%d replies>" % len(out) + b"".join(out)
