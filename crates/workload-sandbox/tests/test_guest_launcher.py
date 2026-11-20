import importlib.machinery
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


launcher = Path(__file__).resolve().parents[1] / "guest" / "remotemedia-space-launch"
loader = importlib.machinery.SourceFileLoader("guest_launcher", str(launcher))
spec = importlib.util.spec_from_loader(loader.name, loader)
guest = importlib.util.module_from_spec(spec)
loader.exec_module(guest)


class ApprovedWheelsTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.wheel_dir = Path(directory.name)
        self.inventory = self.wheel_dir / "approved-wheels.json"
        self.wheel = "fixture-1.0-py3-none-any.whl"
        self.digest = "a" * 64

    def write_inventory(self, wheels, *, version="3.12", abi="cp312"):
        self.inventory.write_text(json.dumps({"version": 2, "python_abi": abi, "python_version": version,
                                              "wheels": wheels}))

    def test_uses_only_approved_local_wheels(self):
        self.write_inventory({self.wheel: self.digest})
        path = self.wheel_dir / self.wheel
        path.touch()
        self.assertEqual(guest.verified_requirements(self.wheel_dir, "gradio", "cp312", (3, 12, 3)),
                         [f"{path.as_uri()} --hash=sha256:{self.digest}"])
        (self.wheel_dir / "unexpected.whl").touch()
        with self.assertRaisesRegex(ValueError, "differs from its approved inventory"):
            guest.verified_requirements(self.wheel_dir, "gradio", "cp312", (3, 12, 3))

    def test_missing_wheel_and_incorrect_abi_are_rejected(self):
        self.write_inventory({self.wheel: self.digest})
        with self.assertRaisesRegex(ValueError, "differs from its approved inventory"):
            guest.verified_requirements(self.wheel_dir, "gradio", "cp312", (3, 12, 3))
        with self.assertRaisesRegex(ValueError, "Python ABI"):
            guest.verified_requirements(self.wheel_dir, "gradio", "cp311", (3, 11, 9))

    def test_rejects_an_invalid_wheel_identity(self):
        self.write_inventory({"https://example.invalid/fixture.whl": self.digest})
        with self.assertRaisesRegex(ValueError, "invalid approved wheel identity"):
            guest.verified_requirements(self.wheel_dir, "gradio", "cp312", (3, 12, 3))

    def test_static_uses_an_empty_inventory(self):
        self.write_inventory({})
        self.assertEqual(guest.verified_requirements(self.wheel_dir, "static", "cp312", (3, 12, 3)), [])
        with self.assertRaisesRegex(ValueError, "Space SDK"):
            guest.verified_requirements(self.wheel_dir, "gradio", "cp312", (3, 12, 3))

    def test_exact_interpreter_version_with_matching_abi(self):
        self.write_inventory({}, version="3.12.3")
        self.assertEqual(guest.verified_requirements(self.wheel_dir, "static", "cp312", (3, 12, 3)), [])
        with self.assertRaisesRegex(ValueError, "guest Python version"):
            guest.verified_requirements(self.wheel_dir, "static", "cp312", (3, 12, 4))

    def test_abi_only_inventory_is_for_source_builds(self):
        self.inventory.write_text(json.dumps({"version": 1, "python_abi": "cp312", "wheels": {}}))
        self.assertEqual(guest.verified_requirements(self.wheel_dir, "build", "cp312", (3, 12, 3)), [])
        with self.assertRaisesRegex(ValueError, "inventory version"):
            guest.verified_requirements(self.wheel_dir, "static", "cp312", (3, 12, 3))

    def test_version_mismatch_never_launches_installer_or_app(self):
        with tempfile.TemporaryDirectory() as workspace:
            source = Path(workspace).resolve()
            entry = source / "app.py"
            entry.touch()
            self.write_inventory({self.wheel: self.digest}, version="3.12.4")
            (self.wheel_dir / self.wheel).touch()
            with (patch.multiple(guest, SOURCE=source, WHEELS=self.wheel_dir),
                  patch.object(guest, "python_target", return_value=("cp312", (3, 12, 3))),
                  patch.object(guest.sys, "argv", ["launcher", "gradio", str(entry), "24623"]),
                  patch.object(guest.os, "execve") as launch, patch.object(guest.subprocess, "run") as uv):
                with self.assertRaisesRegex(ValueError, "guest Python version"):
                    guest.main()
            uv.assert_not_called()
            launch.assert_not_called()

    def test_installer_and_app_receive_explicit_environment(self):
        with tempfile.TemporaryDirectory() as workspace:
            source = Path(workspace) / "source"
            source.mkdir()
            entry = source / "app.py"
            entry.touch()
            scratch = Path(workspace) / "work"
            scratch.mkdir()
            abi = f"cp{guest.sys.version_info.major}{guest.sys.version_info.minor}"
            self.write_inventory({self.wheel: self.digest}, abi=abi,
                                 version=".".join(map(str, guest.sys.version_info[:3])))
            (self.wheel_dir / self.wheel).touch()
            snapshots = []

            def record_environment(*args, env, **kwargs):
                snapshots.append(dict(env))

            with (patch.multiple(guest, SOURCE=source.resolve(), WHEELS=self.wheel_dir, SCRATCH=scratch),
                  patch.object(guest.sys, "argv", ["launcher", "gradio", str(entry), "24623"]),
                  patch.dict(guest.os.environ, {"UV_INDEX_URL": "https://example.invalid", "HF_TOKEN": "fixture"}),
                  patch.object(guest.os, "chdir"), patch.object(guest.os, "execve") as launch,
                  patch.object(guest.subprocess, "run", side_effect=record_environment) as uv):
                guest.main()
            self.assertEqual(uv.call_count, 3)
            for call in uv.call_args_list:
                self.assertEqual(call.args[0][:4], [guest.UV, "--offline", "--no-config", "--no-cache"])
            for env in snapshots:
                self.assertEqual(set(env), {"HOME", "TMPDIR", "PATH"})
            self.assertIn("--no-python-downloads", uv.call_args_list[0].args[0])
            self.assertIn("--require-hashes", uv.call_args_list[1].args[0])
            self.assertIn("--no-index", uv.call_args_list[1].args[0])
            self.assertEqual(launch.call_args.args[2]["HF_HUB_OFFLINE"], "1")
            self.assertNotIn("HF_TOKEN", launch.call_args.args[2])
