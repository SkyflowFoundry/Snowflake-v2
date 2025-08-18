"""Logging configuration for the setup process."""

import logging
import sys
from rich.logging import RichHandler
from rich.console import Console

console = Console()


def setup_logging(level: str = "INFO") -> logging.Logger:
    """Setup logging with Rich handler for beautiful output."""
    
    # Convert string level to logging constant
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    
    # Configure logging
    logging.basicConfig(
        level=numeric_level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)]
    )
    
    return logging.getLogger("skyflow_setup")