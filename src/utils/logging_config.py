"""Centralized logging configuration."""

import logging
import os
import sys
from pythonjsonlogger import jsonlogger
from typing import Optional

def setup_logging(
    level: str = None,
    format_type: str = 'json',
    log_file: Optional[str] = None
) -> logging.Logger:
    """Setup centralized logging configuration."""
    
    # Determine log level
    level = level or os.getenv('LOG_LEVEL', 'INFO')
    log_level = getattr(logging, level.upper())
    
    # Create logger
    logger = logging.getLogger('component_scoring')
    logger.setLevel(log_level)
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Create handler
    if log_file:
        handler = logging.FileHandler(log_file)
    else:
        handler = logging.StreamHandler(sys.stdout)
    
    # Set formatter based on environment
    if format_type == 'json':
        formatter = jsonlogger.JsonFormatter(
            '%(asctime)s %(name)s %(levelname)s %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    # Set up other loggers
    logging.getLogger('google').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    return logger
