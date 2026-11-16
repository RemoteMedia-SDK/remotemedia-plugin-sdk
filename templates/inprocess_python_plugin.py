"""
Template for in-process Python plugins using PyO3 (Android / opt-in Linux/macOS).

This template defines a Python class that can be loaded and executed in-process
via the `remotemedia_plugin_sdk::python_inprocess_plugin_export!` macro.

Key differences from multiprocess (subprocess) plugins:
- Methods are SYNCHRONOUS (no async/await)
- No iceoryx2 IPC - data passed directly via PyO3
- No READY handshake - initialize() blocks until complete
- Runs in the same process as the Rust host (single GIL)

Usage:
1. Copy this file to your plugin crate's `embedded/` directory
2. Use `remotemedia_plugin_sdk::python_inprocess_plugin_export!` in your Rust cdylib
3. The embedded directory is included via `include_dir!`

Generators (nodes that produce text, audio, video or images) must declare
how they stop a cancelled call: derive from `TextGenerator`,
`AudioGenerator`, `VideoGenerator` or `ImageGenerator` as well as
`InProcessNode`, set `cancel_strategy`, and implement `async def cancel`.
The contract below is vendored from the SDK's `remotemedia.core.cancel`.

Example Rust usage:
```rust
use include_dir::{include_dir, Dir};
use remotemedia_plugin_sdk::python_inprocess_plugin_export;

static EMBED: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/embedded");

python_inprocess_plugin_export! {
    node_type: "MyInProcessNode",
    module:    "my_inprocess_plugin",
    class:     "MyInProcessNode",
    embedded:  &EMBED,
}
```
"""

from __future__ import annotations

import abc
import asyncio
import contextvars
import enum
import inspect
import json
import logging
import sys
import threading
import time
import weakref
from contextlib import contextmanager
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Generator,
    Iterator,
    List,
    Mapping,
    Optional,
    Protocol,
    Tuple,
    Union,
    runtime_checkable,
)

#: Reserved aux port of the per-call cancel frame (unused in process: the
#: in-process host calls ``_remotemedia_cancel_call`` instead).
CANCEL_CALL_PORT = "__cancel_call__"
#: Reserved aux port of the node discovery request.
NODE_META_PORT = "__node_meta__"


# ─── Cancellation contract (openspec: add-generator-cancellation) ───────
#
# A verbatim copy of `remotemedia.core.cancel` from the RemoteMedia SDK
# Python package (remotemedia-sdk/clients/python), so this template stays
# importable on its own. Do not edit it here: the SDK's
# tests/test_cancel_template_parity.py fails when the copies differ.
#
# A generator derives from one of TextGenerator / AudioGenerator /
# VideoGenerator / ImageGenerator *and* InProcessNode, sets
# `cancel_strategy`, and implements `async def cancel(self, call,
# cancellation)`; see ExampleTextGenerator below.

# ── BEGIN CONTRACT ── (vendored verbatim into remotemedia-plugin-sdk
# templates/inprocess_python_plugin.py; tests/test_cancel_template_parity.py
# fails if the copies differ)

logger = logging.getLogger("remotemedia.cancel")

#: Reserved aux port of the per-call cancel frame the host sends a worker:
#: ``{"__aux_port__": "__cancel_call__", "payload": {"cancellation": {...}}}``.
CANCEL_CALL_PORT = "__cancel_call__"
#: Reserved aux port of the node discovery request (framed like
#: ``__rpc_meta__``); the reply's value is the :class:`GeneratorDeclaration`.
NODE_META_PORT = "__node_meta__"


# ─── Contract types (mirror crates/traits/schema/cancellation.schema.json) ──


class CancelReason(enum.Enum):
    """Why a call was cancelled."""

    #: A ``barge_in`` (speech over the reply, an interrupt control).
    BARGE_IN = "barge_in"
    #: A newer input replaced the call on a latest-input-wins node.
    SUPERSEDED = "superseded"
    #: The caller cancelled the call (an RPC or endpoint cancel, a dropped
    #: call with no reason given).
    CALLER_CANCELLED = "caller_cancelled"
    #: The input that started the call was retracted (``CancelSpeculation``).
    RETRACTED = "retracted"
    #: The session closed while the call ran.
    SESSION_CLOSED = "session_closed"
    #: The runtime is shutting down.
    SHUTDOWN = "shutdown"
    #: The call missed a deadline.
    DEADLINE = "deadline"

    def default_disposition(self) -> "Disposition":
        """A barge-in or a supersession interrupts; everything else aborts."""
        if self in (CancelReason.BARGE_IN, CancelReason.SUPERSEDED):
            return Disposition.INTERRUPT
        return Disposition.ABORT


class Retention(enum.Enum):
    """Keep or drop one kind of data on a cancel."""

    RETAIN = "retain"
    DISCARD = "discard"


def _enum(kind: type, value: Any, what: str) -> Any:
    if isinstance(value, kind):
        return value
    try:
        return kind(value)  # type: ignore[call-arg]
    except ValueError:
        allowed = ", ".join(repr(m.value) for m in kind)  # type: ignore[attr-defined]
        raise ValueError(f"invalid {what} {value!r}; expected one of {allowed}") from None


@dataclass(frozen=True)
class Disposition:
    """What a cancel does with data."""

    #: What the node does with what it generated before the cancel.
    partial_output: Retention
    #: Input already received or queued for the node, including input the
    #: node buffered internally: kept for the next call, or dropped.
    pending_input: Retention

    #: Stop generating; keep the partial output and the pending input.
    INTERRUPT: ClassVar["Disposition"]
    #: The turn is void: roll back the partial output, drop pending input.
    ABORT: ClassVar["Disposition"]

    def with_override(self, over: Optional["DispositionOverride"]) -> "Disposition":
        """Apply an override's fields on top of this disposition."""
        if over is None:
            return self
        return Disposition(
            partial_output=over.partial_output or self.partial_output,
            pending_input=over.pending_input or self.pending_input,
        )

    def to_dict(self) -> Dict[str, str]:
        return {
            "partial_output": self.partial_output.value,
            "pending_input": self.pending_input.value,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "Disposition":
        if not isinstance(value, Mapping):
            raise ValueError(f"disposition must be an object, got {value!r}")
        missing = [k for k in ("partial_output", "pending_input") if k not in value]
        if missing:
            raise ValueError(f"disposition is missing {', '.join(missing)}")
        return cls(
            partial_output=_enum(Retention, value["partial_output"], "partial_output"),
            pending_input=_enum(Retention, value["pending_input"], "pending_input"),
        )


Disposition.INTERRUPT = Disposition(Retention.RETAIN, Retention.RETAIN)
Disposition.ABORT = Disposition(Retention.DISCARD, Retention.DISCARD)


@dataclass(frozen=True)
class DispositionOverride:
    """A partial disposition, as a canceller (``barge_in`` envelope, RPC
    cancel) or a manifest node's ``on_cancel`` gives it. Absent fields fall
    through."""

    partial_output: Optional[Retention] = None
    pending_input: Optional[Retention] = None

    def is_empty(self) -> bool:
        return self.partial_output is None and self.pending_input is None

    def or_(self, lower: Optional["DispositionOverride"]) -> "DispositionOverride":
        """Fields of ``self`` win; the rest come from ``lower``."""
        if lower is None:
            return self
        return DispositionOverride(
            partial_output=self.partial_output or lower.partial_output,
            pending_input=self.pending_input or lower.pending_input,
        )

    @classmethod
    def of(cls, disposition: Disposition) -> "DispositionOverride":
        return cls(disposition.partial_output, disposition.pending_input)

    def to_dict(self) -> Dict[str, str]:
        out: Dict[str, str] = {}
        if self.partial_output is not None:
            out["partial_output"] = self.partial_output.value
        if self.pending_input is not None:
            out["pending_input"] = self.pending_input.value
        return out

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "DispositionOverride":
        if not isinstance(value, Mapping):
            raise ValueError(f"disposition override must be an object, got {value!r}")
        partial = value.get("partial_output")
        pending = value.get("pending_input")
        return cls(
            partial_output=None if partial is None else _enum(Retention, partial, "partial_output"),
            pending_input=None if pending is None else _enum(Retention, pending, "pending_input"),
        )


def resolve_disposition(
    canceller: Optional[DispositionOverride],
    manifest: Optional[DispositionOverride],
    node_default: Disposition,
) -> Disposition:
    """Resolve a cancel's disposition, per field, in order of precedence: the
    canceller, the manifest node's ``on_cancel``, the node's default (which
    itself starts from the reason's default)."""
    over = (canceller or DispositionOverride()).or_(manifest)
    return node_default.with_override(over)


@dataclass(frozen=True)
class Cancellation:
    """A call's cancellation: why, and what to do with data."""

    reason: CancelReason
    disposition: Disposition

    @classmethod
    def new(cls, reason: Union[CancelReason, str]) -> "Cancellation":
        """A cancellation with the reason's default disposition."""
        r = _enum(CancelReason, reason, "cancel reason")
        return cls(r, r.default_disposition())

    def to_dict(self) -> Dict[str, Any]:
        return {"reason": self.reason.value, "disposition": self.disposition.to_dict()}

    def to_json(self) -> str:
        """The wire form, byte-identical to Rust's ``Cancellation::to_json``."""
        return json.dumps(self.to_dict(), separators=(",", ":"))

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "Cancellation":
        if not isinstance(value, Mapping):
            raise ValueError(f"cancellation must be an object, got {value!r}")
        missing = [k for k in ("reason", "disposition") if k not in value]
        if missing:
            raise ValueError(f"cancellation is missing {', '.join(missing)}")
        return cls(
            reason=_enum(CancelReason, value["reason"], "cancel reason"),
            disposition=Disposition.from_dict(value["disposition"]),
        )

    @classmethod
    def from_json(cls, text: Union[str, bytes, Mapping[str, Any]]) -> "Cancellation":
        if isinstance(text, Mapping):
            return cls.from_dict(text)
        return cls.from_dict(json.loads(text))


@dataclass(frozen=True)
class CancelContext:
    """What a node's ``default_disposition`` is told about a cancel."""

    reason: CancelReason
    #: Seconds since the call started, when the worker knows it.
    elapsed: Optional[float] = None


@dataclass(frozen=True)
class CancelRequest:
    """A cancellation as it crosses a boundary to a node in another process
    (a worker's cancel frame). ``cancellation`` is the host's resolution;
    ``explicit`` is what the canceller and manifest chose, so the node can
    re-resolve with its own default for this cancel (which may depend on
    ``elapsed_ms``, e.g. an early-interrupt rollback)."""

    cancellation: Cancellation
    explicit: DispositionOverride = DispositionOverride()
    elapsed_ms: Optional[int] = None

    @classmethod
    def exact(cls, cancellation: Cancellation) -> "CancelRequest":
        """A request whose disposition is final: the node's default does not
        apply."""
        return cls(cancellation, DispositionOverride.of(cancellation.disposition), None)

    def context(self) -> CancelContext:
        elapsed = None if self.elapsed_ms is None else self.elapsed_ms / 1000.0
        return CancelContext(self.cancellation.reason, elapsed)

    def resolve_with(self, node_default: Disposition) -> Cancellation:
        """The cancellation with the node's own default under the explicit
        choices."""
        return Cancellation(self.cancellation.reason, node_default.with_override(self.explicit))

    def resolve_for(self, node: Any) -> Cancellation:
        """Re-resolve for ``node``: a generator's ``default_disposition``
        under the explicit choices; a transform takes ``cancellation`` as is."""
        if isinstance(node, GeneratorNode):
            return self.resolve_with(node.default_disposition(self.context()))
        return self.cancellation

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"cancellation": self.cancellation.to_dict()}
        if not self.explicit.is_empty():
            out["explicit"] = self.explicit.to_dict()
        if self.elapsed_ms is not None:
            out["elapsed_ms"] = self.elapsed_ms
        return out

    def to_json(self) -> str:
        """The wire form, byte-identical to Rust's ``CancelRequest::to_json``."""
        return json.dumps(self.to_dict(), separators=(",", ":"))

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "CancelRequest":
        """Accepts a ``CancelRequest``, or a bare ``Cancellation``, which is
        taken as :meth:`exact` (as Rust's ``CancelRequest::from_json``)."""
        if not isinstance(value, Mapping):
            raise ValueError(f"cancel request must be an object, got {value!r}")
        if "cancellation" not in value:
            return cls.exact(Cancellation.from_dict(value))
        explicit = value.get("explicit")
        elapsed = value.get("elapsed_ms")
        if elapsed is not None and (isinstance(elapsed, bool) or not isinstance(elapsed, int) or elapsed < 0):
            raise ValueError(f"elapsed_ms must be a non-negative integer, got {elapsed!r}")
        return cls(
            cancellation=Cancellation.from_dict(value["cancellation"]),
            explicit=DispositionOverride() if explicit is None else DispositionOverride.from_dict(explicit),
            elapsed_ms=elapsed,
        )

    @classmethod
    def from_json(cls, text: Union[str, bytes, Mapping[str, Any]]) -> "CancelRequest":
        if isinstance(text, Mapping):
            return cls.from_dict(text)
        return cls.from_dict(json.loads(text))

    @classmethod
    def from_frame_payload(cls, payload: Mapping[str, Any]) -> "CancelRequest":
        """Parse the payload of a ``__cancel_call__`` frame.

        ``{"cancellation": {...}}`` alone is the original frame, whose
        disposition is final (:meth:`exact`). With ``explicit`` or
        ``elapsed_ms`` present it is a :class:`CancelRequest`, re-resolved
        with the node's default. (Rust's ``CancelRequest::from_json`` would
        re-resolve the bare-``cancellation`` shape too; a host that wants the
        node default applied with no explicit choice sends ``elapsed_ms``.)
        """
        if not isinstance(payload, Mapping) or "cancellation" not in payload:
            raise ValueError(f"cancel frame payload must hold `cancellation`, got {payload!r}")
        if "explicit" not in payload and "elapsed_ms" not in payload:
            return cls.exact(Cancellation.from_dict(payload["cancellation"]))
        return cls.from_dict(payload)


def default_dispositions(node: Any) -> Dict[str, Dict[str, str]]:
    """A generator's default disposition for every reason (no elapsed time),
    as the ``default_dispositions`` JSON of its discovery frame."""
    return {
        reason.value: node.default_disposition(CancelContext(reason, None)).to_dict()
        for reason in CancelReason
    }


class CancelStrategy(enum.Enum):
    """How a generator stops a cancelled call."""

    #: All generation happens inside the call (the ``process()`` coroutine or
    #: async generator); dropping it stops the work.
    DROP_STOPS_WORK = "drop_stops_work"
    #: Work outside the call polls or subscribes to the call's token.
    OBSERVES_TOKEN = "observes_token"
    #: The node stops work out of band in ``cancel()`` (abort a remote
    #: request, stop a model thread), and may also observe the token.
    ON_CANCEL = "on_cancel"


class GeneratedModality(enum.Enum):
    """What a generator produces."""

    TEXT = "text"
    AUDIO = "audio"
    #: Includes avatar, lip-sync and motion renderers.
    VIDEO = "video"
    IMAGE = "image"


class GeneratorDeclarationError(ValueError):
    """A generator declaration is not admissible."""


@dataclass(frozen=True)
class GeneratorDeclaration:
    """What a node declares about the generator role before it runs. Both
    ``None`` declares nothing (not a generator)."""

    generates: Optional[GeneratedModality] = None
    cancel: Optional[CancelStrategy] = None

    NONE: ClassVar["GeneratorDeclaration"]

    def is_generator(self) -> bool:
        return self.generates is not None

    def validate(self, node_type: str) -> None:
        """The admission rule: ``generates`` requires ``cancel``."""
        if self.generates is not None and self.cancel is None:
            raise GeneratorDeclarationError(
                f"node `{node_type}` declares that it generates {self.generates.value} "
                "but declares no cancel strategy; generators must implement Cancellable"
            )
        if self.generates is None and self.cancel is not None:
            raise GeneratorDeclarationError(
                f"node `{node_type}` declares a cancel strategy but no generated modality"
            )

    def check_instance(self, node_type: str, actual: "GeneratorDeclaration") -> None:
        """A created node must declare what its registration declared, unless
        the registration declared nothing."""
        if self == GeneratorDeclaration.NONE or self == actual:
            return
        raise GeneratorDeclarationError(
            f"node `{node_type}`: its registration declares {self.to_dict()} "
            f"but the node declares {actual.to_dict()}"
        )

    def to_dict(self) -> Dict[str, str]:
        out: Dict[str, str] = {}
        if self.generates is not None:
            out["generates"] = self.generates.value
        if self.cancel is not None:
            out["cancel"] = self.cancel.value
        return out

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "GeneratorDeclaration":
        if not isinstance(value, Mapping):
            raise ValueError(f"generator declaration must be an object, got {value!r}")
        generates = value.get("generates")
        cancel = value.get("cancel")
        return cls(
            generates=None if generates is None else _enum(GeneratedModality, generates, "generates"),
            cancel=None if cancel is None else _enum(CancelStrategy, cancel, "cancel strategy"),
        )

    @classmethod
    def from_json(cls, text: Union[str, bytes]) -> "GeneratorDeclaration":
        return cls.from_dict(json.loads(text))


GeneratorDeclaration.NONE = GeneratorDeclaration()

#: Schema categories whose nodes are expected to generate. A node in one of
#: these categories that declares no generator role fails the registry audit.
GENERATIVE_CATEGORIES: Tuple[str, ...] = (
    "llm",
    "tts",
    "t2i",
    "video",
    "avatar",
    "motion",
    "text-generation",
)


def is_generative_category(category: str) -> bool:
    return category.lower() in GENERATIVE_CATEGORIES


# ─── Token ──────────────────────────────────────────────────────────────


class _TokenState:
    __slots__ = (
        "lock",
        "event",
        "cancellation",
        "callbacks",
        "waiters",
        "children",
        "next_id",
        "observed",
        "__weakref__",
    )

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.event = threading.Event()
        self.cancellation: Optional[Cancellation] = None
        self.callbacks: Dict[int, Callable[[Cancellation], None]] = {}
        self.waiters: Dict[int, Tuple[asyncio.AbstractEventLoop, "asyncio.Future[Cancellation]"]] = {}
        self.children: List["weakref.ReferenceType[_TokenState]"] = []
        self.next_id = 0
        #: Set once anything observed the fired token (a query, a wait, an
        #: awaited ``cancelled()``, a callback). The conformance check uses it.
        self.observed = False

    def fire(self, cancellation: Cancellation) -> bool:
        with self.lock:
            if self.cancellation is not None:
                return False
            self.cancellation = cancellation
            callbacks = list(self.callbacks.values())
            waiters = list(self.waiters.values())
            children = list(self.children)
            self.callbacks.clear()
            self.waiters.clear()
            self.children.clear()
            self.event.set()
        for callback in callbacks:
            _run_callback(callback, cancellation)
            self.observed = True
        for loop, future in waiters:
            try:
                loop.call_soon_threadsafe(_resolve, future, cancellation)
            except RuntimeError:  # the waiter's loop is closed
                pass
        for ref in children:
            child = ref()
            if child is not None:
                child.fire(cancellation)
        return True


def _run_callback(callback: Callable[[Cancellation], None], cancellation: Cancellation) -> None:
    # A node's callback must not take down the canceller.
    try:
        callback(cancellation)
    except Exception:  # noqa: BLE001 - logged, never propagated
        logger.exception("cancel callback %r raised", callback)


def _resolve(future: "asyncio.Future[Cancellation]", cancellation: Cancellation) -> None:
    if not future.done():
        future.set_result(cancellation)


class CancelRegistration:
    """Keeps an :meth:`CancelToken.on_cancel` callback registered until
    :meth:`unregister` (or the end of a ``with`` block)."""

    __slots__ = ("_state", "_id")

    def __init__(self, state: Optional[_TokenState], key: Optional[int]) -> None:
        self._state = state
        self._id = key

    def unregister(self) -> bool:
        """Remove the callback if it has not run. Returns whether it was removed."""
        state, key = self._state, self._id
        self._state = self._id = None
        if state is None or key is None:
            return False
        with state.lock:
            return state.callbacks.pop(key, None) is not None

    def __enter__(self) -> "CancelRegistration":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.unregister()


class CancelToken:
    """Observes a call's cancellation. Cheap to copy, safe from any thread."""

    __slots__ = ("_state",)
    _NEVER: ClassVar[Optional["CancelToken"]] = None

    def __init__(self, state: Optional[_TokenState] = None) -> None:
        #: ``None`` is a token that never fires.
        self._state = state

    @classmethod
    def never(cls) -> "CancelToken":
        """A token that never fires (the token outside a call, or of a host
        that predates cancellation)."""
        if cls._NEVER is None:
            cls._NEVER = cls(None)
        return cls._NEVER

    def is_cancelled(self) -> bool:
        state = self._state
        if state is None or not state.event.is_set():
            return False
        state.observed = True
        return True

    def cancellation(self) -> Optional[Cancellation]:
        state = self._state
        if state is None or not state.event.is_set():
            return None
        state.observed = True
        return state.cancellation

    def wait(self, timeout: Optional[float] = None) -> bool:
        """Block this thread until the call is cancelled or ``timeout``
        seconds pass (``None`` waits indefinitely; never use that on a token
        that may never fire). Returns whether it was cancelled. For model
        loops on plain threads; never call it on an event loop thread."""
        state = self._state
        if state is None:
            if timeout is None:
                raise ValueError("waiting without a timeout on a token that never fires")
            time.sleep(timeout)
            return False
        fired = state.event.wait(timeout)
        if fired:
            state.observed = True
        return fired

    async def cancelled(self) -> Cancellation:
        """Resolve when the call is cancelled, also when it is cancelled on
        another thread. Never resolves for :meth:`never`."""
        loop = asyncio.get_running_loop()
        state = self._state
        if state is None:
            await loop.create_future()  # never resolves
            raise AssertionError("unreachable")
        future: "asyncio.Future[Cancellation]" = loop.create_future()
        with state.lock:
            if state.cancellation is not None:
                state.observed = True
                return state.cancellation
            key = state.next_id
            state.next_id += 1
            state.waiters[key] = (loop, future)
        try:
            result = await future
            state.observed = True
            return result
        finally:
            with state.lock:
                state.waiters.pop(key, None)

    def on_cancel(self, callback: Callable[[Cancellation], None]) -> CancelRegistration:
        """Run ``callback`` once when the call is cancelled, on the thread
        that cancels it. If it already was, ``callback`` runs now on this
        thread. Exceptions from the callback are logged, never raised."""
        state = self._state
        if state is None:
            return CancelRegistration(None, None)
        with state.lock:
            fired = state.cancellation
            if fired is None:
                key = state.next_id
                state.next_id += 1
                state.callbacks[key] = callback
                return CancelRegistration(state, key)
        _run_callback(callback, fired)
        state.observed = True
        return CancelRegistration(None, None)

    def child(self) -> "CancelSource":
        """A source for sub-work: it fires when this token fires (with the
        same cancellation) or when it is cancelled itself."""
        child = CancelSource()
        state = self._state
        if state is None:
            return child
        with state.lock:
            fired = state.cancellation
            if fired is None:
                state.children = [c for c in state.children if c() is not None]
                state.children.append(weakref.ref(child._state))
        if fired is not None:
            child.cancel(fired)
        return child

    @property
    def observed(self) -> bool:
        """Whether anything observed this token after it fired."""
        return self._state is not None and self._state.observed

    def __repr__(self) -> str:
        if self._state is None:
            return "CancelToken.never()"
        return f"CancelToken(cancellation={self._state.cancellation!r})"


class CancelSource:
    """Fires a call's cancellation. Held by whoever may cancel the call (the
    worker, or a node cancelling its own sub-work through
    :meth:`CancelToken.child`)."""

    __slots__ = ("_state",)

    def __init__(self) -> None:
        self._state = _TokenState()

    def token(self) -> CancelToken:
        return CancelToken(self._state)

    def cancel(self, cancellation: Union[Cancellation, CancelReason, str]) -> bool:
        """Fire the cancellation. Only the first call has an effect; returns
        whether this one did. Callbacks run on the calling thread."""
        if not isinstance(cancellation, Cancellation):
            cancellation = Cancellation.new(cancellation)
        return self._state.fire(cancellation)

    def is_cancelled(self) -> bool:
        return self._state.event.is_set()

    def cancellation(self) -> Optional[Cancellation]:
        return self._state.cancellation

    @property
    def observed(self) -> bool:
        return self._state.observed

    def __repr__(self) -> str:
        return f"CancelSource(cancellation={self._state.cancellation!r})"


# ─── Generator role ─────────────────────────────────────────────────────


@dataclass(frozen=True)
class CallInfo:
    """The call a cancel applies to."""

    session_id: str
    node_id: str
    #: The worker's id for the call, when it has one.
    call_id: Optional[str] = None


@runtime_checkable
class Cancellable(Protocol):
    """The cancellation contract, structurally. :class:`GeneratorNode` is the
    nominal form every Python generator uses."""

    cancel_strategy: ClassVar[CancelStrategy]

    def default_disposition(self, ctx: CancelContext) -> Disposition: ...

    async def cancel(self, call: CallInfo, cancellation: Cancellation) -> None: ...


def _class_name(cls: type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


def _check_not_legacy(cls: type) -> None:
    # Looked up, not imported: this module is imported by the node bases.
    legacy = getattr(sys.modules.get("remotemedia.core.node"), "Node", None)
    if not isinstance(legacy, type) or not issubclass(cls, legacy):
        return
    multiprocess = getattr(sys.modules.get("remotemedia.core.multiprocessing.node"), "MultiprocessNode", None)
    if isinstance(multiprocess, type) and issubclass(cls, multiprocess):
        return
    raise TypeError(
        f"generator {_class_name(cls)} derives from the legacy remotemedia.core.node.Node base; "
        "generators must use MultiprocessNode (or the in-process InProcessNode)"
    )


def _other_abstract_methods(cls: type) -> List[str]:
    names: "set[str]" = set()
    for base in cls.__mro__[1:]:
        names.update(getattr(base, "__abstractmethods__", ()))
    names.update(n for n, v in vars(cls).items() if getattr(v, "__isabstractmethod__", False))
    return sorted(
        n for n in names
        if n != "cancel" and getattr(getattr(cls, n, None), "__isabstractmethod__", False)
    )


class GeneratorNode(abc.ABC):
    """Mixin for nodes that generate text, audio, video or images.

    Combine it with the node base: ``class MyTts(AudioGenerator,
    MultiprocessNode)`` (or ``InProcessNode`` in the in-process template).
    A concrete subclass must set ``cancel_strategy`` and implement
    ``async def cancel``; otherwise defining it raises ``TypeError``. An
    intermediate base that leaves them to its subclasses passes
    ``abstract=True``: ``class _Base(AudioGenerator, MultiprocessNode,
    abstract=True)``.
    """

    #: What the node generates. Set by the modality bases, or filled by
    #: ``@register_node(..., generates=...)``.
    generates: ClassVar[GeneratedModality]
    #: How the node stops a cancelled call.
    cancel_strategy: ClassVar[CancelStrategy]

    def __init_subclass__(cls, abstract: bool = False, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        _check_not_legacy(cls)
        if abstract or _other_abstract_methods(cls):
            return
        problems: List[str] = []
        cancel = getattr(cls, "cancel", None)
        if cancel is None or getattr(cancel, "__isabstractmethod__", False):
            problems.append("implement `async def cancel(self, call, cancellation)`")
        elif not inspect.iscoroutinefunction(cancel):
            problems.append("make `cancel` an `async def`")
        strategy = getattr(cls, "cancel_strategy", None)
        if not isinstance(strategy, CancelStrategy):
            problems.append("set `cancel_strategy` to a CancelStrategy")
        generates = getattr(cls, "generates", None)
        if generates is not None and not isinstance(generates, GeneratedModality):
            problems.append("set `generates` to a GeneratedModality")
        if problems:
            raise TypeError(f"generator {_class_name(cls)} must " + " and ".join(problems))

    @abc.abstractmethod
    async def cancel(self, call: CallInfo, cancellation: Cancellation) -> None:
        """Stop out-of-band work and apply ``cancellation.disposition`` to the
        node's own state. Runs once per cancelled call, after the call's token
        fired and possibly concurrently with the tail of the call; must be
        idempotent and must not raise (exceptions are logged)."""

    def default_disposition(self, ctx: CancelContext) -> Disposition:
        """The disposition this node wants when neither the canceller nor the
        manifest chose one. Defaults to the reason's."""
        return ctx.reason.default_disposition()

    @property
    def cancel_token(self) -> CancelToken:
        """The current call's token; a never-firing token outside a call."""
        return current_cancel_token(self)


class TextGenerator(GeneratorNode, abstract=True):
    generates = GeneratedModality.TEXT


class AudioGenerator(GeneratorNode, abstract=True):
    generates = GeneratedModality.AUDIO


class VideoGenerator(GeneratorNode, abstract=True):
    """Includes avatar, lip-sync and motion renderers."""

    generates = GeneratedModality.VIDEO


class ImageGenerator(GeneratorNode, abstract=True):
    generates = GeneratedModality.IMAGE


def declaration_of(node: Any) -> GeneratorDeclaration:
    """The generator declaration of a node class or instance (``NONE`` for a
    transform)."""
    cls = node if isinstance(node, type) else type(node)
    if not issubclass(cls, GeneratorNode):
        return GeneratorDeclaration.NONE
    generates = getattr(cls, "generates", None)
    strategy = getattr(cls, "cancel_strategy", None)
    return GeneratorDeclaration(
        generates=generates if isinstance(generates, GeneratedModality) else None,
        cancel=strategy if isinstance(strategy, CancelStrategy) else None,
    )


def node_meta(node: Any) -> Dict[str, Any]:
    """The value of a ``__node_meta__`` reply: the generator declaration
    (``{}`` for a transform) plus, for a generator, ``default_dispositions``."""
    meta: Dict[str, Any] = dict(declaration_of(node).to_dict())
    if isinstance(node, GeneratorNode):
        meta["default_dispositions"] = default_dispositions(node)
    return meta


# ─── Worker side: the current call ──────────────────────────────────────


class CallScope:
    """A worker's state for one call: its token source and bookkeeping. One
    per ``process()`` invocation."""

    __slots__ = (
        "node",
        "info",
        "source",
        "started",
        "work",
        "cancel_delivered",
        "cancelled_at",
        "suppressed",
        "__weakref__",
    )

    def __init__(self, node: Any, info: CallInfo) -> None:
        self.node = node
        self.info = info
        self.source = CancelSource()
        self.started = time.monotonic()
        #: The task running the call, when the worker can drop it.
        self.work: Optional["asyncio.Future[Any]"] = None
        self.cancel_delivered = False
        self.cancelled_at: Optional[float] = None
        #: Monotonic times of outputs the call produced after its cancel
        #: (suppressed by the worker).
        self.suppressed: List[float] = []

    @property
    def token(self) -> CancelToken:
        return self.source.token()

    def context(self, reason: CancelReason) -> CancelContext:
        return CancelContext(reason, time.monotonic() - self.started)

    def fire(self, cancellation: Cancellation) -> bool:
        fired = self.source.cancel(cancellation)
        if fired:
            self.cancelled_at = time.monotonic()
        return fired


_CURRENT_CALL: "contextvars.ContextVar[Optional[CallScope]]" = contextvars.ContextVar(
    "remotemedia_current_call", default=None
)


def current_call() -> Optional[CallScope]:
    """The call the current task (or a ``to_thread`` job of it) belongs to."""
    return _CURRENT_CALL.get()


@contextmanager
def enter_call(scope: CallScope) -> Iterator[CallScope]:
    """Make ``scope`` the current call for this context and for the node's
    plain threads (which do not inherit context variables)."""
    reset = _CURRENT_CALL.set(scope)
    previous = getattr(scope.node, "_rm_active_call", None)
    try:
        scope.node._rm_active_call = scope
    except AttributeError:
        pass
    try:
        yield scope
    finally:
        _CURRENT_CALL.reset(reset)
        try:
            if getattr(scope.node, "_rm_active_call", None) is scope:
                scope.node._rm_active_call = previous
        except AttributeError:
            pass


def current_cancel_token(node: Any) -> CancelToken:
    """The token of ``node``'s current call, or :meth:`CancelToken.never`."""
    scope = _CURRENT_CALL.get()
    if scope is not None and scope.node is node:
        return scope.token
    scope = getattr(node, "_rm_active_call", None)
    if isinstance(scope, CallScope):
        return scope.token
    return CancelToken.never()


async def deliver_cancel(
    node: Any,
    scope: CallScope,
    cancellation: Cancellation,
    log: Optional[logging.Logger] = None,
) -> bool:
    """Fire the call's token, then run the generator's ``cancel()`` once (as
    the current call). Returns whether ``cancel()`` ran. Exceptions from
    ``cancel()`` are logged, never raised."""
    scope.fire(cancellation)
    if not isinstance(node, GeneratorNode) or scope.cancel_delivered:
        return False
    scope.cancel_delivered = True
    fired = scope.source.cancellation() or cancellation
    with enter_call(scope):
        try:
            await node.cancel(scope.info, fired)
        except Exception:  # noqa: BLE001 - a node's cancel must not break the worker
            (log or logger).exception("%s.cancel() raised", type(node).__name__)
    return True


# ── END CONTRACT ──


# Type aliases for clarity
ConfigDict = Dict[str, Any]
RuntimeData = Any  # In practice, this is the Rust RuntimeData enum passed via PyO3


class InProcessNode:
    """
    Base class for in-process Python nodes.
    
    All methods are synchronous - they run on the Python GIL directly.
    
    The data passed to `process()` and returned from it will be converted
    between Rust's `RuntimeData` enum and Python types automatically by
    the PyO3 bridge. You receive native Python types:
    
    - Text → str
    - Audio → (numpy.ndarray, sample_rate: int, channels: int)
    - Video → dict with keys: 'data', 'width', 'height', 'format', 'codec'
    - Image → dict with keys: 'data', 'width', 'height', 'format'
    - Tensor → numpy.ndarray
    - Numpy → numpy.ndarray
    - Json → dict/list
    - Binary → bytes
    - ControlMessage → dict
    - File → dict with keys: 'path', 'metadata'
    
    Return the same types from `process()` for automatic conversion back.
    """

    def __init__(self) -> None:
        """Optional: Initialize any instance-level state (not configuration)."""
        ...

    def initialize(self, config: ConfigDict) -> None:
        """
        Initialize the node with configuration.
        
        Called once after the node is created. Use this to:
        - Load ML models (torch, transformers, etc.)
        - Initialize connections
        - Allocate resources
        
        BLOCKS the host thread until complete - suitable for heavy model loading.
        No READY signal needed; the node is ready when this returns.
        
        Args:
            config: Dictionary of configuration parameters from the pipeline manifest.
                   Contains keys from the node's `params` field.
        
        Raises:
            Exception: If initialization fails (will be caught and reported to Rust)
        """
        # Example: Load a model
        # import torch
        # self.model = torch.jit.load(config.get("model_path", "default.pt"))
        pass

    def process(self, data: RuntimeData) -> Union[RuntimeData, None]:
        """
        Process a single input and return a single output.
        
        This is the main processing method. Called for each input item.
        
        Args:
            data: Input data (native Python type based on RuntimeData variant)
        
        Returns:
            Output data (native Python type, will be converted to RuntimeData),
            or None if no output is produced.
        
        Raises:
            Exception: If processing fails (will be caught and reported to Rust)
        """
        # Example: Echo text input
        # if isinstance(data, str):
        #     return f"Echo: {data}"
        # return data
        return data

    def process_streaming(self, data: RuntimeData) -> Generator[RuntimeData, None, None]:
        """
        Process a single input and yield multiple outputs (streaming).
        
        Implement this OR `process()`, not both. If implemented, the Rust
        side will use this method for multi-output nodes.
        
        Args:
            data: Input data (native Python type based on RuntimeData variant)
        
        Yields:
            Output data items (native Python types)
        
        Raises:
            Exception: If processing fails
        """
        # Example: Stream multiple chunks
        # if isinstance(data, str):
        #     for word in data.split():
        #         yield word
        #     return
        # yield data
        yield data

    # ── Host hooks for per-call cancellation ──────────────────────────────
    #
    # The in-process host (pyo3) runs each call through `_remotemedia_invoke`
    # so the call gets its own cancellation token, and cancels it with
    # `_remotemedia_cancel_call` from the thread where the host cancels.
    # Authors do not call these; a generator reads `self.cancel_token`.

    #: The call in flight (set by `_remotemedia_invoke`).
    _rm_inflight: Optional["CallScope"] = None

    def _remotemedia_invoke(self, method: str, data: RuntimeData) -> Any:
        """Run ``process`` (or another method) as one call with its own token.

        For ``process_streaming``, returns a generator that stops yielding a
        generator node's outputs once its call is cancelled.
        """
        scope = CallScope(self, CallInfo(
            session_id=getattr(self, "session_id", "") or "",
            node_id=getattr(self, "node_id", "") or "",
            call_id=f"inprocess#{id(self):x}:{time.monotonic_ns()}",
        ))
        self._rm_inflight = scope
        if method == "process_streaming":
            return self._remotemedia_stream(scope, data)
        try:
            with enter_call(scope):
                result = getattr(self, method)(data)
            if isinstance(self, GeneratorNode) and scope.source.is_cancelled():
                return None
            return result
        finally:
            if getattr(self, "_rm_inflight", None) is scope:
                self._rm_inflight = None

    def _remotemedia_stream(self, scope: "CallScope", data: RuntimeData) -> Iterator[RuntimeData]:
        drops = isinstance(self, GeneratorNode)
        try:
            with enter_call(scope):
                outputs = self.process_streaming(data)
            while True:
                with enter_call(scope):
                    try:
                        output = next(outputs)
                    except StopIteration:
                        return
                if drops and scope.source.is_cancelled():
                    outputs.close()
                    return
                yield output
        finally:
            if getattr(self, "_rm_inflight", None) is scope:
                self._rm_inflight = None

    def _remotemedia_cancel_call(self, request_json: str) -> bool:
        """Cancel the call in flight: fire its token with the cancellation
        (re-resolved with a generator's ``default_disposition`` when the
        host sends a CancelRequest), then run a generator's ``cancel()``.
        Returns whether a call was in flight. Safe from any thread."""
        scope = getattr(self, "_rm_inflight", None)
        if scope is None:
            return False
        request = CancelRequest.from_json(request_json)
        cancellation = request.resolve_for(self)
        if not scope.fire(cancellation) or not isinstance(self, GeneratorNode):
            return True
        coroutine = deliver_cancel(self, scope, cancellation)
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(coroutine)  # no loop on this thread: run cancel() here
        else:
            # Called on a thread running a loop: schedule it there.
            asyncio.ensure_future(coroutine)
        return True

    def _remotemedia_node_meta(self) -> str:
        """The discovery JSON: generator declaration and default dispositions."""
        return json.dumps(node_meta(self), separators=(",", ":"))

    @property
    def cancel_token(self) -> "CancelToken":
        """The current call's token; a never-firing token outside a call."""
        return current_cancel_token(self)

    def finalize(self) -> None:
        """
        Clean up resources before shutdown.
        
        Called once when the node is being destroyed. Use this to:
        - Release model references
        - Close connections
        - Save state
        
        No async operations - runs synchronously on the GIL.
        Exceptions here are logged but not propagated.
        """
        # Example: Release model
        # if hasattr(self, 'model'):
        #     del self.model
        pass


# Example concrete implementation
class ExampleEchoNode(InProcessNode):
    """
    Example: Simple echo node for testing in-process execution.
    """
    
    def __init__(self) -> None:
        super().__init__()
        self.counter = 0
    
    def initialize(self, config: ConfigDict) -> None:
        # Could load a model here if needed
        self.prefix = config.get("prefix", "Echo")
        print(f"[ExampleEchoNode] Initialized with prefix: {self.prefix}")
    
    def process(self, data: RuntimeData) -> Union[RuntimeData, None]:
        self.counter += 1
        
        # Handle different data types
        if isinstance(data, str):
            return f"{self.prefix} #{self.counter}: {data}"
        elif isinstance(data, bytes):
            return f"{self.prefix} #{self.counter}: {data.decode('utf-8', errors='replace')}".encode()
        elif isinstance(data, dict):
            # JSON or ControlMessage
            data["_echo_count"] = self.counter
            data["_echo_prefix"] = self.prefix
            return data
        else:
            # Pass through other types (Audio, Video, Tensor, etc.)
            return data
    
    def finalize(self) -> None:
        print(f"[ExampleEchoNode] Finalized after {self.counter} messages")


class ExampleStreamingNode(InProcessNode):
    """
    Example: Streaming node that yields multiple outputs per input.
    """
    
    def initialize(self, config: ConfigDict) -> None:
        self.chunk_size = config.get("chunk_size", 100)
    
    def process_streaming(self, data: RuntimeData) -> Generator[RuntimeData, None, None]:
        if isinstance(data, str):
            # Stream word by word
            for word in data.split():
                yield word
        elif isinstance(data, bytes):
            # Stream chunks
            for i in range(0, len(data), self.chunk_size):
                yield data[i:i + self.chunk_size]
        else:
            # Single output for non-streamable types
            yield data
    
    def finalize(self) -> None:
        pass


class ExampleTextGenerator(TextGenerator, InProcessNode):
    """
    Example: a generator. It declares how it stops (`cancel_strategy`) and
    implements `cancel()`; leaving either out raises TypeError when the
    class is defined. Its synchronous loop observes the call's token.
    """

    cancel_strategy = CancelStrategy.OBSERVES_TOKEN

    def initialize(self, config: ConfigDict) -> None:
        self.history: List[str] = []
        self._partial: List[str] = []

    def process_streaming(self, data: RuntimeData) -> Generator[RuntimeData, None, None]:
        token = self.cancel_token
        self._partial = []
        for word in str(data).split():
            if token.is_cancelled():
                return
            self._partial.append(word)
            yield word
        self.history.append(" ".join(self._partial))
        self._partial = []

    async def cancel(self, call: CallInfo, cancellation: Cancellation) -> None:
        # Apply the disposition to the node's own state: keep the cut-off
        # reply (interrupt) or roll it back (abort).
        if self._partial and cancellation.disposition.partial_output is Retention.RETAIN:
            self.history.append(" ".join(self._partial) + " [interrupted]")
        self._partial = []

    def finalize(self) -> None:
        pass


# This module can be used as a template for user plugins
if __name__ == "__main__":
    # Quick self-test when run directly
    node = ExampleEchoNode()
    node.initialize({"prefix": "Test"})
    result = node.process("hello world")
    print(f"Result: {result}")
    node.finalize()
    
    print("\n--- Streaming test ---")
    stream_node = ExampleStreamingNode()
    stream_node.initialize({"chunk_size": 5})
    for chunk in stream_node.process_streaming("hello world streaming"):
        print(f"Chunk: {chunk}")
    stream_node.finalize()