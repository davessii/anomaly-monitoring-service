"""Read this process's Windows cycle counter. No CPU-time conversion.

Unsupported systems explicitly return no measurement; never synthesize zero.
GetCurrentProcess returns a pseudo handle, which does not need CloseHandle.
"""
import ctypes
import os


class ProcessCycles:
    def __init__(self):
        self.source = 'QueryProcessCycleTime'
        self.available = os.name == 'nt'
        self._query = None
        self._handle = None
        if self.available:
            from ctypes import wintypes as w
            kernel = ctypes.WinDLL('kernel32', use_last_error=True)
            kernel.GetCurrentProcess.restype = w.HANDLE
            kernel.QueryProcessCycleTime.argtypes = [w.HANDLE, ctypes.POINTER(ctypes.c_ulonglong)]
            kernel.QueryProcessCycleTime.restype = w.BOOL
            self._query = kernel.QueryProcessCycleTime
            self._handle = kernel.GetCurrentProcess()

    def read(self):
        if not self.available:
            return None
        value = ctypes.c_ulonglong()
        if not self._query(self._handle, ctypes.byref(value)):
            raise ctypes.WinError(ctypes.get_last_error())
        return value.value


def validate_bracket(before, after):
    if any(type(value) is not int or value < 0 for value in (before, after)):
        raise ValueError('Cycle values must be nonnegative integers.')
    if after < before:
        raise ValueError('Cycle counter decreased within snapshot.')
