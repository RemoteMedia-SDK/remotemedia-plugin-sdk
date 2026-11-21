import ctypes
import importlib.machinery
import importlib.util
import os
from pathlib import Path
import stat
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from uuid import UUID


launcher = Path(__file__).resolve().parents[1] / "guest" / "remotemedia-cuda-verify"
loader = importlib.machinery.SourceFileLoader("guest_cuda", str(launcher))
spec = importlib.util.spec_from_loader(loader.name, loader)
guest = importlib.util.module_from_spec(spec)
loader.exec_module(guest)

ASSIGNED = UUID("11111111-1111-1111-1111-111111111111")


class GuestCudaTest(unittest.TestCase):
    def driver(self, *, count=1, uuid=ASSIGNED):
        driver = Mock()
        for function in ("cuInit", "cuDeviceGet", "cuDevicePrimaryCtxRetain", "cuCtxSetCurrent",
                         "cuMemAlloc_v2", "cuMemFree_v2", "cuDevicePrimaryCtxRelease_v2"):
            getattr(driver, function).return_value = 0

        def enumerate_devices(pointer):
            pointer._obj.value = count
            return 0

        def identify(pointer, _device):
            ctypes.memmove(pointer, uuid.bytes, len(uuid.bytes))
            return 0

        def upload(_device_pointer, host, size):
            self.sent = ctypes.string_at(host, size)
            return 0

        def download(host, _device_pointer, size):
            ctypes.memmove(host, self.sent, size)
            return 0

        driver.cuDeviceGetCount.side_effect = enumerate_devices
        driver.cuDeviceGetUuid_v2.side_effect = identify
        driver.cuMemcpyHtoD_v2.side_effect = upload
        driver.cuMemcpyDtoH_v2.side_effect = download
        return driver

    def test_cuda_requires_exactly_the_assigned_uuid_before_allocation(self):
        for driver, reason in ((self.driver(count=2), "exactly one"),
                               (self.driver(uuid=UUID(int=7)), "UUID differs")):
            with patch.object(guest.c, "CDLL", return_value=driver):
                with self.assertRaisesRegex(RuntimeError, reason):
                    guest.check_cuda(f"GPU-{ASSIGNED}")
            driver.cuMemAlloc_v2.assert_not_called()

    def test_verifier_checks_round_trip_and_releases_device_context(self):
        driver = self.driver()
        with patch.object(guest.c, "CDLL", return_value=driver):
            guest.check_cuda(f"GPU-{ASSIGNED}")
        driver.cuMemcpyHtoD_v2.assert_called_once()
        driver.cuMemcpyDtoH_v2.assert_called_once()
        driver.cuMemFree_v2.assert_called_once()
        driver.cuDevicePrimaryCtxRelease_v2.assert_called_once()

    def test_extra_gpu_node_prevents_launcher_dispatch(self):
        with (patch.object(sys, "argv", [str(launcher), f"GPU-{ASSIGNED}", "7", "/trusted/launch"]),
              patch.object(os, "listdir", return_value=["nvidia7", "nvidiactl", "nvidia-uvm", "nvidia1"]),
              patch.object(guest, "check_cuda") as cuda, patch.object(os, "execv") as execute):
            with self.assertRaisesRegex(RuntimeError, "unexpected NVIDIA device nodes"):
                guest.main()
        cuda.assert_not_called()
        execute.assert_not_called()

    def test_guest_uvm_uses_sentry_major_and_exact_nvidia_minors(self):
        devices = {"nvidia7": (195, 7), "nvidiactl": (195, 255), "nvidia-uvm": (240, 0)}

        def metadata(path, **_options):
            major, minor = devices[path.removeprefix("/dev/")]
            return SimpleNamespace(st_mode=stat.S_IFCHR, st_rdev=(major << 32) | minor)

        with (patch.object(os, "listdir", return_value=list(devices)),
              patch.object(os, "stat", side_effect=metadata),
              patch.object(os, "major", create=True, side_effect=lambda device: device >> 32),
              patch.object(os, "minor", create=True, side_effect=lambda device: device & 0xFFFFFFFF)):
            guest.check_nodes(7)
            devices["nvidia7"] = (195, 8)
            with self.assertRaisesRegex(RuntimeError, "device number differs"):
                guest.check_nodes(7)


if __name__ == "__main__":
    unittest.main()
