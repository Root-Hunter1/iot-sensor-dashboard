"""Batch UDP receive.

On Linux, RecvMMsgReader calls libc's recvmmsg() through ctypes. One system
call returns up to `vlen` datagrams. A plain recv() loop needs one system call
per datagram, which costs more CPU when packets arrive quickly.

On other systems, or if recvmmsg cannot be loaded, SimpleReader is used.
Both readers have the same read() method, so the rest of the code does not care
which one is active.
"""
from __future__ import annotations

import ctypes
import ctypes.util
import errno
import logging
import os
import socket
import sys

log = logging.getLogger("fastrecv")

MSG_DONTWAIT = 0x40  # Linux value: do not block even if the socket is in blocking mode


# These match the C structs in <sys/socket.h>. Field order and types must be exact.
class iovec(ctypes.Structure):
    _fields_ = [("iov_base", ctypes.c_void_p), ("iov_len", ctypes.c_size_t)]


class msghdr(ctypes.Structure):
    _fields_ = [
        ("msg_name", ctypes.c_void_p),
        ("msg_namelen", ctypes.c_uint32),
        ("msg_iov", ctypes.POINTER(iovec)),
        ("msg_iovlen", ctypes.c_size_t),
        ("msg_control", ctypes.c_void_p),
        ("msg_controllen", ctypes.c_size_t),
        ("msg_flags", ctypes.c_int),
    ]


class mmsghdr(ctypes.Structure):
    _fields_ = [("msg_hdr", msghdr), ("msg_len", ctypes.c_uint)]


def _load_recvmmsg():
    """Return the libc recvmmsg function, or raise OSError if it is missing."""
    for name in (ctypes.util.find_library("c"), "libc.so.6", "libc.so"):
        if not name:
            continue
        try:
            libc = ctypes.CDLL(name, use_errno=True)
            fn = libc.recvmmsg
        except (OSError, AttributeError):
            continue
        fn.argtypes = [ctypes.c_int, ctypes.POINTER(mmsghdr), ctypes.c_uint,
                       ctypes.c_int, ctypes.c_void_p]
        fn.restype = ctypes.c_int
        return fn
    raise OSError("recvmmsg is not available in libc")


class RecvMMsgReader:
    """Reads up to `vlen` datagrams per call. Each datagram is truncated to `bufsize` bytes.

    All message buffers live in one contiguous arena, allocated once. Each packet
    is copied out with a memoryview slice, which measured faster in Python than
    one ctypes buffer per message.
    """

    kind = "recvmmsg"

    def __init__(self, fd: int, vlen: int, bufsize: int):
        self._recvmmsg = _load_recvmmsg()
        self._fd = fd
        self._vlen = vlen
        self._bufsize = bufsize
        self._arena = (ctypes.c_char * (vlen * bufsize))()
        self._view = memoryview(self._arena).cast("B")
        base = ctypes.addressof(self._arena)
        self._iovs = (iovec * vlen)()
        self._msgs = (mmsghdr * vlen)()
        for i in range(vlen):
            self._iovs[i].iov_base = base + i * bufsize
            self._iovs[i].iov_len = bufsize
            hdr = self._msgs[i].msg_hdr
            hdr.msg_name = None      # we do not need the sender address
            hdr.msg_namelen = 0
            hdr.msg_iov = ctypes.pointer(self._iovs[i])
            hdr.msg_iovlen = 1

    def read(self) -> list[bytes]:
        n = self._recvmmsg(self._fd, self._msgs, self._vlen, MSG_DONTWAIT, None)
        if n < 0:
            err = ctypes.get_errno()
            if err in (errno.EAGAIN, errno.EINTR):
                return []   # nothing to read right now
            raise OSError(err, os.strerror(err))
        bs, view, msgs = self._bufsize, self._view, self._msgs
        return [view[i * bs: i * bs + msgs[i].msg_len].tobytes() for i in range(n)]


class SimpleReader:
    """Portable fallback: one recv() per datagram, up to `vlen` per call."""

    kind = "simple"

    def __init__(self, sock: socket.socket, vlen: int, bufsize: int):
        self._sock = sock
        self._vlen = vlen
        self._bufsize = bufsize

    def read(self) -> list[bytes]:
        out: list[bytes] = []
        for _ in range(self._vlen):
            try:
                out.append(self._sock.recv(self._bufsize))
            except BlockingIOError:
                break
        return out


def make_reader(sock: socket.socket, kind: str = "recvmmsg",
                vlen: int = 64, bufsize: int = 513):
    """Pick the best available reader. The socket must be non-blocking."""
    if kind == "recvmmsg":
        if sys.platform.startswith("linux"):
            try:
                return RecvMMsgReader(sock.fileno(), vlen, bufsize)
            except OSError as exc:
                log.warning("recvmmsg unavailable (%s), using plain recv", exc)
        else:
            log.warning("recvmmsg is Linux-only, using plain recv")
    return SimpleReader(sock, vlen, bufsize)
