"""Run against an installed wheel, never against a Python replacement exporter."""
import gc
import importlib
from importlib.machinery import ExtensionFileLoader
import unittest
import numpy as np
import _remotemedia_buffers as buffers


class InstalledWheel(unittest.TestCase):
    def test_native_module_and_version(self):
        # Maturin installs a package shim which re-exports the native module.
        native = importlib.import_module('_remotemedia_buffers._remotemedia_buffers')
        self.assertIsInstance(native.__spec__.loader, ExtensionFileLoader)
        self.assertIs(buffers.ReadBuffer, native.ReadBuffer)
        self.assertEqual(buffers.BUFFER_PROTOCOL_VERSION, 1)

    def test_numpy_output_and_escaped_slice(self):
        loan = buffers.local_allocator().allocate_audio(16)
        array = np.asarray(loan)
        pointer = array.__array_interface__['data'][0]
        np.multiply(np.arange(16, dtype=np.float32), np.float32(2), out=array)
        escaped = array[4:8]
        array.setflags(write=False)
        del array
        with self.assertRaises(BufferError):
            loan.publish()
        del escaped
        gc.collect()
        result = np.asarray(loan.publish())
        self.assertEqual(result.__array_interface__['data'][0], pointer)
        self.assertEqual(result.tolist(), [float(i * 2) for i in range(16)])
        self.assertFalse(result.flags.writeable)
        with self.assertRaises(ValueError):
            result.setflags(write=True)

    def test_readonly_byte_view_keeps_published_owner(self):
        loan = buffers.local_allocator().allocate_bytes(8)
        writable = memoryview(loan)
        writable[:] = b'abcdefgh'
        writable.release()
        published = loan.publish()
        retained = memoryview(published)[2:6]
        del loan, published
        gc.collect()
        self.assertTrue(retained.readonly)
        self.assertEqual(retained.tobytes(), b'cdef')  # Test comparison only.


if __name__ == '__main__':
    unittest.main()
