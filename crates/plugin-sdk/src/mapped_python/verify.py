"""Host preflight: exact installed wheel identities and owning-buffer protocol."""
import contextlib
import importlib
import importlib.metadata
import json
import os
from pathlib import Path
import sys


def verify_distribution(name, expected_hash, modules):
    distribution = importlib.metadata.distribution(name)
    source = json.loads(distribution.read_text("direct_url.json") or "{}")
    archive = source.get("archive_info", {})
    digest = archive.get("hashes", {}).get("sha256")
    if digest is None:
        digest = archive.get("hash", "").removeprefix("sha256=")
    if digest != expected_hash:
        raise RuntimeError(f"{name}: installed wheel hash does not match selected artifact")
    prefix = Path(sys.prefix).resolve()
    if prefix != Path(sys.argv[3]).resolve():
        raise RuntimeError("interpreter does not use selected virtual environment")
    files = {Path(distribution.locate_file(item)).resolve() for item in distribution.files or ()}
    for name in modules:
        module = importlib.import_module(name)
        path = Path(module.__file__).resolve()
        if path not in files or not path.is_relative_to(prefix):
            raise RuntimeError(f"{name}: imported outside its selected distribution")


with contextlib.redirect_stdout(sys.stderr):
    verify_distribution("remotemedia-buffers", sys.argv[2], ["_remotemedia_buffers"])
    import _remotemedia_buffers as buffers
    if (buffers.BUFFER_PROTOCOL_VERSION, buffers.WORKER_PROTOCOL_VERSION) != (1, 1):
        raise RuntimeError("incompatible mapped buffer/worker protocol")
    verify_distribution("remotemedia-client", sys.argv[1], [
        "remotemedia_mapped_worker", "remotemedia.core.multiprocessing.mapped_runner",
        "remotemedia.core.multiprocessing.mapped_data",
        "remotemedia.core.multiprocessing.mapped_context",
    ])
    from remotemedia.core.multiprocessing import mapped_runner
    if getattr(mapped_runner, "MAPPED_RUNNER_PROTOCOL_VERSION", None) != 3:
        raise RuntimeError("incompatible mapped Python runner protocol")
    snapshots = int(sys.argv[4]) if len(sys.argv) > 4 else 0
    if snapshots not in (0, 1, 2):
        raise RuntimeError("unknown mapped Python snapshot protocol")
    output_copies = int(sys.argv[5]) if len(sys.argv) > 5 else 0
    if output_copies not in (0, 1):
        raise RuntimeError("unknown mapped Python output copy protocol")
    if output_copies:
        from remotemedia.core.multiprocessing import mapped_context
        if (getattr(buffers, "OUTPUT_COPY_PROTOCOL_VERSION", None),
                getattr(mapped_context, "OUTPUT_COPY_PROTOCOL_VERSION", None)) != (1, 1):
            raise RuntimeError("incompatible mapped Python output copy protocol")
    if snapshots >= 1:
        if (getattr(buffers, "SNAPSHOT_PROTOCOL_VERSION", None),
                getattr(mapped_runner, "MAPPED_SNAPSHOT_PROTOCOL_VERSION", None)) != (1, 1):
            raise RuntimeError("incompatible mapped Python snapshot protocol")
    if snapshots == 2:
        verify_distribution("remotemedia-client", sys.argv[1], [
            "remotemedia.core.multiprocessing.mapped_snapshots",
        ])
        if (getattr(buffers, "SNAPSHOT_OUTPUT_PROTOCOL_VERSION", None),
                getattr(mapped_runner, "MAPPED_SNAPSHOT_OUTPUT_PROTOCOL_VERSION", None)) != (2, 2):
            raise RuntimeError("incompatible mapped Python snapshot output protocol")
print(json.dumps({"base_executable": sys._base_executable, "pid": os.getpid()}))
