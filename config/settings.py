
# import os
# import yaml
# from typing import Dict, Any, Optional
# from pathlib import Path
# from dataclasses import dataclass

# @dataclass
# class DatabaseConfig:
#     """Database configuration settings."""
#     project_id: str
#     dataset_id: str
#     max_bytes_billed: int
#     use_query_cache: bool = True

# @dataclass  
# class ProcessingConfig:
#     """Processing pipeline configuration."""
#     batch_size: int
#     max_workers: int
#     memory_limit_gb: int
#     sampling_enabled: bool = False
#     sample_size: int = 10000

# class Settings:
#     """Centralized configuration management."""
    
#     def __init__(self, env: str = None):
#         self.env = env or os.getenv('ENVIRONMENT', 'development')
#         self.config_dir = Path(__file__).parent
#         self._load_configs()
    
#     def _load_configs(self):
#         """Load configuration files."""
#         # Load main pipeline config
#         with open(self.config_dir / 'pipeline_config.yaml', 'r') as f:
#             self.pipeline_config = yaml.safe_load(f)
        
#         # Load feature engineering config
#         with open(self.config_dir / 'feature_config.yaml', 'r') as f:
#             self.feature_config = yaml.safe_load(f)
            
#         # Load weights config
#         with open(self.config_dir / 'weights.yaml', 'r') as f:
#             self.weights_config = yaml.safe_load(f)
        
#         # Apply environment-specific overrides
#         if self.env in self.pipeline_config.get('environments', {}):
#             env_config = self.pipeline_config['environments'][self.env]
#             self._deep_merge(self.pipeline_config, env_config)
    
#     def _deep_merge(self, base: Dict, override: Dict):
#         """Deep merge configuration dictionaries."""
#         for key, value in override.items():
#             if key in base and isinstance(base[key], dict) and isinstance(value, dict):
#                 self._deep_merge(base[key], value)
#             else:
#                 base[key] = value
    
#     @property
#     def database(self) -> DatabaseConfig:
#         """Get database configuration."""
#         db_config = self.pipeline_config['data_sources']['bigquery']
#         return DatabaseConfig(
#             project_id=os.getenv('GOOGLE_CLOUD_PROJECT', db_config['project_id']),
#             dataset_id=db_config['dataset_id'],
#             max_bytes_billed=db_config['max_bytes_billed'],
#             use_query_cache=db_config['use_query_cache']
#         )
    
#     @property
#     def processing(self) -> ProcessingConfig:
#         """Get processing configuration."""
#         proc_config = self.pipeline_config['processing']
#         sampling_config = proc_config.get('sampling', {})
        
#         return ProcessingConfig(
#             batch_size=proc_config['batch_size'],
#             max_workers=proc_config['max_workers'], 
#             memory_limit_gb=proc_config['memory_limit_gb'],
#             sampling_enabled=sampling_config.get('enabled', False),
#             sample_size=sampling_config.get('sample_size', 10000)
#         )
    
#     def get_weights(self, variant: str = 'base_weights') -> Dict[str, float]:
#         """Get scoring weights by variant."""
#         if variant == 'base_weights':
#             return self.weights_config['base_weights']
#         return self.weights_config['weight_variants'].get(variant, self.weights_config['base_weights'])

# part_priority_scoring/config/settings.py
"""Configuration management for the module."""

import yaml
from pathlib import Path
from typing import Dict, Any

def get_default_config() -> Dict[str, Any]:
    """Get default configuration."""
    config_dir = Path(__file__).parent
    
    # Load feature config
    with open(config_dir / 'feature_config.yaml', 'r') as f:
        feature_config = yaml.safe_load(f)
    
    # Load weights
    with open(config_dir / 'weights.yaml', 'r') as f:
        weights_config = yaml.safe_load(f)
    
    return {
        'features': feature_config,
        'weights': weights_config['base_weights'],
        'project_id': None,  # To be set by user
        'dataset': 'datadojo.part_priority_scoring'
    }

def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)