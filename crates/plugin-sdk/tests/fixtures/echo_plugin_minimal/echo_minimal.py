# /// script
# dependencies = []
# ///

"""Minimal echo module — fixture for `provision_real_venv_smoke`.

Declares zero PEP 723 deps so the provisioning test stays fast and
self-contained. The actual node logic is irrelevant — the test only
exercises the cdylib → extract → ensure_env path, not subprocess
execution."""


def python_requires(deps):
    """No-op decorator stand-in. Lets the file be import-safe outside
    the runner context while still exposing the `@python_requires([])`
    literal the parser scans for."""
    def decorator(cls):
        return cls
    return decorator


@python_requires([])
class EchoMinimal:
    """Placeholder class — not actually instantiated by the test."""

    pass
