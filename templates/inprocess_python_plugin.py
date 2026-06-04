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

from typing import Any, Dict, Generator, Iterator, Optional, Union


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