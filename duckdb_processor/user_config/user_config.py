"""
User configuration file management.

This module provides functionality for loading and saving user
preferences to configuration files.
"""

import json
from pathlib import Path
from typing import Optional


class UserConfig:
    """
    User configuration for DuckDB CSV Processor.

    Attributes:
        formatter_type: Default formatter ('rich' or 'simple')
        color_enabled: Whether color output is enabled
        progress_enabled: Whether progress indicators are shown
        theme: Color theme ('default', 'high-contrast', 'monochrome')
    """

    def __init__(
        self,
        formatter_type: str = "rich",
        color_enabled: bool = True,
        progress_enabled: bool = True,
        theme: str = "default"
    ):
        self.formatter_type = formatter_type
        self.color_enabled = color_enabled
        self.progress_enabled = progress_enabled
        self.theme = theme

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            'formatter_type': self.formatter_type,
            'color_enabled': self.color_enabled,
            'progress_enabled': self.progress_enabled,
            'theme': self.theme
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'UserConfig':
        """Create instance from dictionary."""
        return cls(
            formatter_type=data.get('formatter_type', 'rich'),
            color_enabled=data.get('color_enabled', True),
            progress_enabled=data.get('progress_enabled', True),
            theme=data.get('theme', 'default')
        )


def get_user_config_path() -> Path:
    """
    Get path to user configuration file.

    Returns:
        Path to config file (may not exist)
    """
    config_dir = Path.home() / '.config' / 'duckdb-processor'
    return config_dir / 'config.json'


def load_user_config() -> Optional[UserConfig]:
    """
    Load user configuration from file.

    Returns:
        UserConfig instance if file exists, None otherwise
    """
    config_path = get_user_config_path()

    if not config_path.exists():
        return None

    try:
        with open(config_path, 'r') as f:
            data = json.load(f)
        return UserConfig.from_dict(data)
    except (json.JSONDecodeError, IOError):
        return None
