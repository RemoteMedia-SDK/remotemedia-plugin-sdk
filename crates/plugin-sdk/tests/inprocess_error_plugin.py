"""Test plugin for in-process Python execution - Error plugin."""

class ErrorPlugin:
    """Plugin that raises errors for testing error propagation."""
    
    def initialize(self, config: dict):
        """Initialize the plugin."""
        self.initialized = True
        return True
    
    def process(self, data: dict) -> dict:
        """Process input - raises error if 'trigger error' in data."""
        if isinstance(data, dict):
            data_str = str(data)
        else:
            data_str = str(data)
        
        if "trigger error" in data_str.lower():
            raise RuntimeError("Test error from Python plugin!")
        
        return data
    
    def finalize(self):
        """Cleanup."""
        self.initialized = False
        return True