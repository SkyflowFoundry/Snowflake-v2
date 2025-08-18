"""Input validation utilities."""

import re
from typing import List, Tuple, Optional


def validate_prefix(prefix: str) -> Tuple[bool, Optional[str]]:
    """Validate prefix name meets Snowflake naming requirements."""
    if not prefix:
        return False, "Prefix cannot be empty"
    
    if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', prefix):
        return False, "Prefix must start with a letter and contain only letters, numbers, and underscores"
    
    if len(prefix) > 50:
        return False, "Prefix cannot be longer than 50 characters"
    
    # Reserved keywords
    reserved = ['system', 'information_schema', 'public', 'snowflake', 'util_db']
    if prefix.lower() in reserved:
        return False, f"Prefix '{prefix}' is reserved and cannot be used"
    
    return True, None


def validate_warehouse_id(warehouse_id: str) -> Tuple[bool, Optional[str]]:
    """Validate warehouse ID format."""
    if not warehouse_id:
        return False, "Warehouse ID cannot be empty"
    
    # Basic format validation - Snowflake warehouse names are alphanumeric with underscores
    if not re.match(r'^[a-zA-Z0-9\-_]{10,}$', warehouse_id):
        return False, "Warehouse ID format appears invalid"
    
    return True, None


def validate_url(url: str, name: str = "URL") -> Tuple[bool, Optional[str]]:
    """Validate URL format."""
    if not url:
        return False, f"{name} cannot be empty"
    
    if not re.match(r'^https?://', url):
        return False, f"{name} must start with http:// or https://"
    
    return True, None


def validate_required_files(file_paths: List[str]) -> Tuple[bool, List[str]]:
    """Validate that required files exist."""
    import os
    from pathlib import Path
    
    missing_files = []
    # Get templates directory relative to this module
    template_dir = Path(__file__).parent.parent / "templates"
    
    for file_path in file_paths:
        # Check in templates directory
        template_path = template_dir / file_path
        if not template_path.exists():
            missing_files.append(file_path)
    
    return len(missing_files) == 0, missing_files