"""
UC-State: Unity Catalog State Management

A utility for managing Databricks resource state using Unity Catalog tables.
"""

from .state_manager import UCState, create_state_manager, add

__version__ = "1.0.0"
__author__ = "Nick Karpov"
__all__ = ['UCState', 'create_state_manager', 'add']