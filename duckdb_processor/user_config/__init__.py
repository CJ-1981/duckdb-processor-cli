"""
User configuration management for DuckDB CSV Processor.

This module provides functionality for managing persistent user
preferences and configuration files.
"""

from .user_config import UserConfig, get_user_config_path, load_user_config

__all__ = [
    "UserConfig",
    "get_user_config_path",
    "load_user_config",
]
