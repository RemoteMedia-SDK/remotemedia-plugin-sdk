"""Test plugin for in-process Python execution - Streaming plugin."""

class StreamingPlugin:
    """Plugin that supports streaming output."""
    
    def initialize(self, config: dict):
        """Initialize the plugin."""
        self.initialized = True
        return True
    
    def process(self, data: dict) -> dict:
        """Process input data and return first item."""
        count = data.get('count', 1)
        return {"index": 0, "value": 0}
    
    def process_streaming(self, data: dict):
        """Stream multiple outputs based on count."""
        count = data.get('count', 1)
        results = []
        for i in range(count):
            results.append({"index": i, "value": i * 10})
        return results
    
    def finalize(self):
        """Cleanup."""
        self.initialized = False
        return True