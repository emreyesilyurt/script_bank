"""Configuration management for the module."""

import yaml
from pathlib import Path
from typing import Dict, Any

def get_default_config() -> Dict[str, Any]:
    """Get default configuration for part scoring."""
    
    config_dir = Path(__file__).parent
    
    # Try to load YAML configs, fall back to defaults if not found
    try:
        with open(config_dir / 'feature_config.yaml', 'r') as f:
            feature_config = yaml.safe_load(f)
    except (FileNotFoundError, yaml.YAMLError):
        feature_config = _get_default_feature_config()
    
    try:
        with open(config_dir / 'weights.yaml', 'r') as f:
            weights_config = yaml.safe_load(f)
    except (FileNotFoundError, yaml.YAMLError):
        weights_config = _get_default_weights_config()
    
    return {
        'features': feature_config,
        'weights': weights_config.get('base_weights', weights_config),
        'project_id': None,  # To be set by user
        'dataset': 'datadojo.part_priority_scoring'
    }

def _get_default_feature_config() -> Dict[str, Any]:
    """Default feature configuration."""
    return {
        'log_transforms': ['inventory', 'first_price', 'moq'],
        'inverse_transforms': ['leadtime_weeks', 'first_price', 'moq'],
        'binary_features': ['is_authorized', 'has_datasheet', 'in_stock', 'immediate_availability'],
        'composite_features': ['availability_score', 'demand_score']
    }

def _get_default_weights_config() -> Dict[str, Any]:
    """Default weights configuration."""
    return {
        'demand_score': 0.25,
        'availability_score': 0.25,
        'inv_leadtime_weeks': 0.15,
        'inv_first_price': 0.10,
        'inv_moq': 0.10,
        'is_authorized': 0.10,
        'has_datasheet': 0.05
    }

def load_config_file(config_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)
