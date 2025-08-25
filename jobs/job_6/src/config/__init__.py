"""
Configuration package for AWS Glue Python Shell jobs.
"""

from .config_loader import load_config, load_ini_config

__all__ = ['load_config', 'load_ini_config'] 