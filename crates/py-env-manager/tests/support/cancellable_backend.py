"""Offline PEP 517 fixture: stop real installers in a selected build hook."""
import base64
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import zipfile

ROOT = Path(__file__).resolve().parent
DIST = "rm_cancel_fixture-1.0.dist-info"
METADATA = b"Metadata-Version: 2.1\nName: rm-cancel-fixture\nVersion: 1.0\n"
WHEEL = b"Wheel-Version: 1.0\nGenerator: cancellation-fixture\nRoot-Is-Purelib: true\nTag: py3-none-any\n"


def hold_tree(depth):
    # Locks are held for the entire interval in which this fixture may write.
    with (ROOT / f"lifetime-{depth}").open("w+b") as lock:
        lock.write(b"0")
        lock.flush()
        lock.seek(0)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(lock.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        deadline = time.monotonic() + 45
        if depth < 2:
            child = subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve()), "descendant", str(depth + 1)],
                stdin=subprocess.DEVNULL,
                start_new_session=os.name != "nt",
            )
            while not (ROOT / f"ready-{depth + 1}").exists():
                if time.monotonic() > deadline or child.poll() is not None:
                    raise RuntimeError("installer descendant startup failed")
                time.sleep(0.01)
        (ROOT / f"ready-{depth}").write_text(str(os.getpid()))
        while time.monotonic() < deadline:
            if (ROOT / "after-cancel").exists():
                target = Path((ROOT / "target").read_text())
                (target / f"late-installer-{depth}").write_text("still running")
            time.sleep(0.01)
    raise RuntimeError("installer fixture watchdog expired")


def barrier(phase):
    (ROOT / f"observed-{phase}").write_text(json.dumps({"pid": os.getpid(), "python": sys.executable}))
    if (ROOT / "phase").read_text() == phase:
        hold_tree(0)


def get_requires_for_build_wheel(config_settings=None):
    barrier("requirements")
    return []


def prepare_metadata_for_build_wheel(metadata_directory, config_settings=None):
    barrier("metadata")
    path = Path(metadata_directory) / DIST
    path.mkdir(exist_ok=True)
    (path / "METADATA").write_bytes(METADATA)
    (path / "WHEEL").write_bytes(WHEEL)
    return DIST


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    barrier("wheel")
    files = {
        "rm_cancel_fixture.py": b"VALUE = 42\n",
        f"{DIST}/METADATA": METADATA,
        f"{DIST}/WHEEL": WHEEL,
    }
    record = io.StringIO(newline="")
    writer = csv.writer(record)
    for name, data in files.items():
        digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
        writer.writerow([name, "sha256=" + digest, len(data)])
    writer.writerow([f"{DIST}/RECORD", "", ""])
    files[f"{DIST}/RECORD"] = record.getvalue().encode()
    name = "rm_cancel_fixture-1.0-py3-none-any.whl"
    with zipfile.ZipFile(Path(wheel_directory) / name, "w") as wheel:
        for path, data in files.items():
            wheel.writestr(path, data)
    return name


if __name__ == "__main__":
    assert sys.argv[1] == "descendant"
    hold_tree(int(sys.argv[2]))
