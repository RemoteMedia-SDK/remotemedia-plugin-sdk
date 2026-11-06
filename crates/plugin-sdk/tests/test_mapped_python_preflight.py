"""Exercise rejection gates against actual installed SDK and buffer wheels.

Set MAPPED_PREFLIGHT_VENV, MAPPED_CLIENT_WHEEL and MAPPED_BUFFER_WHEEL after the
managed factory integration test has provisioned its environment.
"""
import hashlib
import os
from pathlib import Path
import subprocess
import unittest


@unittest.skipUnless(os.environ.get("MAPPED_PREFLIGHT_VENV"), "requires a provisioned test venv")
class InstalledPreflightTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.venv = Path(os.environ["MAPPED_PREFLIGHT_VENV"])
        cls.python = cls.venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        cls.verifier = Path(__file__).resolve().parents[1] / "src/mapped_python/verify.py"
        cls.hashes = []
        for name in ("MAPPED_CLIENT_WHEEL", "MAPPED_BUFFER_WHEEL"):
            with open(os.environ[name], "rb") as artifact:
                cls.hashes.append(hashlib.file_digest(artifact, "sha256").hexdigest())

    def probe(self, prefix="", client_hash=None, venv=None, snapshots=False):
        wrapper = prefix + "\nimport runpy,sys; sys.argv=sys.argv[1:]; runpy.run_path(sys.argv[0], run_name='__main__')"
        return subprocess.run([
            str(self.python), "-I", "-c", wrapper, str(self.verifier),
            client_hash or self.hashes[0], self.hashes[1], str(venv or self.venv), str(int(snapshots)),
        ], capture_output=True, text=True, timeout=30)

    def assert_rejected(self, expected, **options):
        result = self.probe(**options)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(expected, result.stderr)

    def test_matching_installed_artifacts(self):
        result = self.probe()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_matching_snapshot_artifacts(self):
        result = self.probe(snapshots=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_matching_output_artifacts(self):
        result = self.probe(snapshots=2)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_missing_native_output_protocol(self):
        self.assert_rejected("incompatible mapped Python snapshot output protocol", snapshots=2,
                             prefix="import _remotemedia_buffers as b; del b.SNAPSHOT_OUTPUT_PROTOCOL_VERSION")

    def test_incompatible_runner_output_protocol(self):
        self.assert_rejected("incompatible mapped Python snapshot output protocol", snapshots=2,
                             prefix="import remotemedia.core.multiprocessing.mapped_runner as r; r.MAPPED_SNAPSHOT_OUTPUT_PROTOCOL_VERSION = 999")

    def test_shadowed_output_module(self):
        self.assert_rejected("imported outside its selected distribution", snapshots=2,
                             prefix="import remotemedia.core.multiprocessing.mapped_snapshots as m; m.__file__ = '/unselected/mapped_snapshots.py'")

    def test_incompatible_native_snapshot_protocol(self):
        self.assert_rejected("incompatible mapped Python snapshot protocol", snapshots=True,
                             prefix="import _remotemedia_buffers as b; b.SNAPSHOT_PROTOCOL_VERSION = 999")

    def test_missing_runner_snapshot_protocol(self):
        self.assert_rejected("incompatible mapped Python snapshot protocol", snapshots=True,
                             prefix="import remotemedia.core.multiprocessing.mapped_runner as r; del r.MAPPED_SNAPSHOT_PROTOCOL_VERSION")

    def test_snapshot_capability_is_optional_for_ordinary_calls(self):
        result = self.probe(prefix="import _remotemedia_buffers as b; del b.SNAPSHOT_PROTOCOL_VERSION")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_shadowed_snapshot_context(self):
        self.assert_rejected("imported outside its selected distribution", snapshots=True,
                             prefix="import remotemedia.core.multiprocessing.mapped_context as m; m.__file__ = '/unselected/mapped_context.py'")

    def test_different_artifact_hash(self):
        self.assert_rejected("installed wheel hash does not match", client_hash="0" * 64)

    def test_incompatible_buffer_protocol(self):
        self.assert_rejected("incompatible mapped buffer/worker protocol", prefix=
                             "import _remotemedia_buffers as b; b.BUFFER_PROTOCOL_VERSION = 999")

    def test_incompatible_runner_protocol(self):
        self.assert_rejected("incompatible mapped Python runner protocol", prefix=
                             "import remotemedia.core.multiprocessing.mapped_runner as r; r.MAPPED_RUNNER_PROTOCOL_VERSION = 999")

    def test_runner_without_background_drain_is_rejected(self):
        self.assert_rejected("incompatible mapped Python runner protocol", prefix=
                             "import remotemedia.core.multiprocessing.mapped_runner as r; r.MAPPED_RUNNER_PROTOCOL_VERSION = 1")

    def test_runner_without_blocking_job_ownership_is_rejected(self):
        self.assert_rejected("incompatible mapped Python runner protocol", prefix=
                             "import remotemedia.core.multiprocessing.mapped_runner as r; r.MAPPED_RUNNER_PROTOCOL_VERSION = 2")

    def test_shadowed_sdk_module(self):
        self.assert_rejected("imported outside its selected distribution", prefix=
                             "import remotemedia.core.multiprocessing.mapped_data as m; m.__file__ = '/unselected/mapped_data.py'")

    def test_wrong_environment(self):
        self.assert_rejected("does not use selected virtual environment", venv=self.venv / "unselected")


if __name__ == "__main__":
    unittest.main()
