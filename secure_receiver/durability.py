"""Durability primitives that behave correctly on native Windows.

The commit protocol depends on two facts:

* a file's bytes survive a crash once ``os.fsync`` returns for that file, and
* a directory entry (file name) survives once the directory itself is flushed.

POSIX lets you ``os.open(dir)`` and ``os.fsync`` it.  Windows does not open
directories as ordinary files; you must request ``FILE_FLAG_BACKUP_SEMANTICS``
and call ``FlushFileBuffers``.  We do that through ``ctypes`` (stdlib only),
and fall back to a POSIX directory fsync elsewhere so the same code is
testable on any host.
"""

from __future__ import annotations

import ctypes
import os
import sys
from pathlib import Path

IS_WINDOWS = sys.platform.startswith("win")

if IS_WINDOWS:
    from ctypes import wintypes

    _GENERIC_READ = 0x80000000
    _GENERIC_WRITE = 0x40000000
    _FILE_SHARE_READ = 0x00000001
    _FILE_SHARE_WRITE = 0x00000002
    _FILE_SHARE_DELETE = 0x00000004
    _OPEN_EXISTING = 3
    _FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
    _INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value
    _ERROR_ACCESS_DENIED = 5

    # FlushFileBuffers on a *directory* handle requires write access on the
    # directory; FILE_READ_DATA-only handles are denied (ERROR_ACCESS_DENIED)
    # on Windows 11.  We try read+write first, then a list-directory access
    # mask, then a plain write-only open.
    _DIR_ACCESS_MODES = (
        _GENERIC_READ | _GENERIC_WRITE,
        0x0001 | 0x0002 | 0x0004 | 0x0020,  # LIST/ADD_FILE/ADD_SUBDIR/TRAVERSE
        _GENERIC_WRITE,
    )

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

    _CreateFileW = _kernel32.CreateFileW
    _CreateFileW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    _CreateFileW.restype = wintypes.HANDLE

    _FlushFileBuffers = _kernel32.FlushFileBuffers
    _FlushFileBuffers.argtypes = [wintypes.HANDLE]
    _FlushFileBuffers.restype = wintypes.BOOL

    _CloseHandle = _kernel32.CloseHandle
    _CloseHandle.argtypes = [wintypes.HANDLE]
    _CloseHandle.restype = wintypes.BOOL

    _CreateFileW.restype = ctypes.c_void_p
    _FlushFileBuffers.argtypes = [ctypes.c_void_p]
    _CloseHandle.argtypes = [ctypes.c_void_p]


def fsync_file(fd: int) -> None:
    """Flush a file descriptor's data and metadata to stable storage."""
    os.fsync(fd)


# On Windows, os.open() without O_BINARY opens in *text* mode and translates
# 0x0A <-> 0x0D0A in the kernel, which silently corrupts binary segments.  We
# always set it for the low-level opens the store performs.
O_BINARY = getattr(os, "O_BINARY", 0)


def open_binary(path: os.PathLike | str, flags: int, mode: int = 0o600) -> int:
    """os.open with binary mode forced on Windows."""
    return os.open(os.fspath(path), flags | O_BINARY, mode)


def fsync_dir(path: os.PathLike | str) -> None:
    """Flush a directory so new/renamed entries in it are durable."""
    p = os.fspath(path)
    if IS_WINDOWS:
        last_err = 0
        for access in _DIR_ACCESS_MODES:
            handle = _CreateFileW(
                p,
                access,
                _FILE_SHARE_READ | _FILE_SHARE_WRITE | _FILE_SHARE_DELETE,
                None,
                _OPEN_EXISTING,
                _FILE_FLAG_BACKUP_SEMANTICS,
                None,
            )
            if not handle or handle == _INVALID_HANDLE_VALUE:
                last_err = ctypes.get_last_error()
                continue
            try:
                if _FlushFileBuffers(handle):
                    return
                last_err = ctypes.get_last_error()
                if last_err != _ERROR_ACCESS_DENIED:
                    raise OSError(last_err, f"FlushFileBuffers failed for directory {p!r}")
            finally:
                _CloseHandle(handle)
        raise OSError(last_err, f"could not flush directory {p!r}")
    else:
        fd = os.open(p, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def write_file_durably(path: os.PathLike | str, data: bytes, *, flush_dir: bool = True) -> None:
    """Create ``path`` with ``data``; flush the file (and optionally its dir).

    Callers that write many files into one directory should pass
    ``flush_dir=False`` and issue a single :func:`fsync_dir` afterwards, which
    is both sufficient for durability and far cheaper.
    """
    path = Path(path)
    fd = open_binary(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
    try:
        view = memoryview(data)
        while view:
            written = os.write(fd, view)
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)
    if flush_dir:
        fsync_dir(path.parent)
