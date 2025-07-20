import logging
import sys
from datetime import datetime
from pythonjsonlogger import jsonlogger

def setup_logger(name: str, level: str = "INFO") -> logging.Logger:
    """Setup structured JSON logger"""
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    if not logger.handlers:
        # Console handler with JSON format
        console_handler = logging.StreamHandler(sys.stdout)
        json_formatter = jsonlogger.JsonFormatter(
            fmt='%(asctime)s %(name)s %(levelname)s %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(json_formatter)
        logger.addHandler(console_handler)
        
        # File handler for persistent logs
        try:
            file_handler = logging.FileHandler('/app/logs/migration.log')
            file_handler.setFormatter(json_formatter)
            logger.addHandler(file_handler)
        except:
            # If file logging fails, continue with console only
            pass
    
    return logger 