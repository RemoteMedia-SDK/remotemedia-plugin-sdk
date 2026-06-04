"""Test plugin for in-process Python execution - Echo plugin."""

class EchoPlugin:
    """Simple echo plugin that returns input as output."""
    
    def initialize(self, config: dict):
        """Initialize the plugin."""
        self.initialized = True
        return True
    
    def process(self, data: dict) -> dict:
        """Process input data and return it unchanged (echo)."""
        return data
    
    def process_streaming(self, data: dict):
        """Stream the same data multiple times."""
        count = data.get('count', 1)
        results = []
        for i in range(count):
            results.append({"index": i, "value": i * 10})
        return results
    
    def finalize(self):
        """Cleanup."""
        self.initialized = False
        return True