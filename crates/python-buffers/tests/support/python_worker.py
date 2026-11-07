"""Mapped protocol fixture; also runnable against the installed native wheel."""
import sys

# Windows stdout belongs exclusively to the binary protocol. Import-time Python
# diagnostics from third-party modules must use stderr too.
sys.stdout = sys.stderr
import json
import threading
import time
import _remotemedia_buffers as buffers

endpoint = buffers._connect_worker([
    "Describe", "Initialize", "Process", "Stream", "MultiInput", "Tick", "Control", "Rpc"
])
try:
    buffers._connect_worker(["Process"])
except RuntimeError:
    pass
else:
    raise AssertionError("a second endpoint claim was accepted")

# Imports and native stdout writes must work after the transport reader starts.
# The binding redirects application fd 0/1 after claiming its private handles.
import numpy as np
import os
assert sys.stdin.read() == ""
os.write(1, b"mapped Python application stdout is separate from protocol\n")

retained = []


def adapt(request, operation):
    if operation in ("adapt_forward", "adapt_forward_endian"):
        metadata, owner = request.input("input")
        source = np.asarray(owner)[2:6]
        if operation == "adapt_forward_endian":
            source = source.view(">f4")
    else:
        metadata = json.dumps({"Audio": {"sample_rate": 48000, "channels": 1}})
        source = np.arange(8, dtype=np.float32)
        if operation == "adapt_strided":
            source = source[::-2]
        elif operation == "adapt_readonly":
            source.setflags(write=False)
        elif operation == "adapt_f64":
            source = source.astype(np.float64)
        elif operation == "adapt_endian":
            source = source.astype(">f4")
        elif operation == "adapt_empty":
            source = source[:0]
        elif operation == "adapt_heap":
            loan = buffers.local_allocator().allocate_audio(8)
            writable = np.asarray(loan)
            writable[:] = source
            del writable
            source = np.asarray(loan.publish())
    output = np.asarray(request.adapt_audio(source))
    assert not output.flags.writeable
    if operation == "adapt_forward":
        assert output.ctypes.data == source.ctypes.data
    elif source.size:
        assert output.ctypes.data != source.ctypes.data
    assert np.array_equal(output, source)
    request.emit(metadata, output)


def execute(request):
    error = None
    try:
        operation = json.loads(request.arguments_json)["operation"]
        if operation == "rpc":
            request.emit(json.dumps({"Json": json.loads(request.context_json)}))
        elif operation.startswith("adapt_"):
            adapt(request, operation)
        elif operation == "release":
            retained.clear()
        elif operation == "source":
            loan = request.allocator.allocate_bytes(16)
            array = np.asarray(loan)
            array[:] = np.arange(16, dtype=np.uint8)
            pointer = array.ctypes.data
            del array
            published = np.asarray(loan.publish())
            assert published.ctypes.data == pointer
            request.emit('"Binary"', published)
        elif operation == "stream":
            loan = request.allocator.allocate_bytes(16)
            array = np.asarray(loan)
            array[:] = np.arange(16, dtype=np.uint8)
            del array
            published = np.asarray(loan.publish())
            request.emit('"Binary"', published[2:6])
            request.emit('"Binary"', published[8:12])
        elif operation == "wait":
            request.emit('{"Text":"started"}')
            while not request.is_cancelled():
                time.sleep(0.001)
        else:
            assert request.input_names == ["input"]
            metadata, owner = request.input("input")
            array = np.asarray(owner) if owner is not None else None
            if array is not None:
                assert not array.flags.writeable
            if operation == "hold":
                retained.append(array[1:3])
            elif operation == "pass":
                request.emit(metadata, array)
            elif operation == "transform":
                audio = isinstance(json.loads(metadata), dict) and "Audio" in json.loads(metadata)
                allocator = request.allocator
                loan = allocator.allocate_audio(len(array)) if audio else allocator.allocate_bytes(len(array))
                output = np.asarray(loan)
                np.multiply(array, 2, out=output)
                escaped = output[1:3]
                output.setflags(write=False)
                del output
                try:
                    loan.publish()
                except BufferError:
                    pass
                else:
                    raise AssertionError("published with an escaped writable export")
                del escaped
                request.emit(metadata, np.asarray(loan.publish()))
            elif operation == "private":
                request.emit(metadata, np.array(array))
            elif operation == "strided":
                request.emit(metadata, array[::2])
            elif operation == "bad_shape":
                request.emit('{"Tensor":{"shape":[999],"dtype":0,"encoding":"Runtime","metadata":null}}', array)
            else:
                raise ValueError(operation)
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
    finally:
        request.complete(error)


while True:
    request = endpoint.next_request()
    threading.Thread(target=execute, args=(request,), daemon=True).start()
    del request
