"""Configuration module for loading YAML config files."""
import yaml

def load_config(path: str):
    """Load YAML configuration file.
    
    Args:
        path: Path to the YAML configuration file
        
    Returns:
        Dictionary containing the configuration
    """
    with open(path, "r") as file:
        return yaml.safe_load(file)
