#!/usr/bin/env python3
"""
FilenameUtils.py

Utility class for filename operations used across multiple steps in the pipeline.
This class contains static methods for creating safe filenames and related operations.
"""

import re
import hashlib


class FilenameUtils:
    """
    Utility class for filename operations.
    All methods are static as this class is intended to be used as a collection
    of related utility functions rather than instantiated objects.
    """
    
    @staticmethod
    def create_safe_filename(*, vendor_name, list_source):
        """
        Create a safe filename from the vendor name and list_source by:
        1. Replacing special characters with spaces
        2. Converting groups of spaces to underscores
        3. Converting to lowercase
        4. Appending MD5 hash of list_source for uniqueness
        
        Args:
            vendor_name (str): The original vendor name to convert
            list_source (str): The source URL to hash for uniqueness
            
        Returns:
            str: A safe filename string suitable for file system use
        """
        # Replace all non-alphanumeric characters with spaces
        safe_name = re.sub(r'[^a-zA-Z0-9\s]', ' ', vendor_name)
        
        # Convert multiple spaces to single spaces, then to underscores
        safe_name = re.sub(r'\s+', '_', safe_name.strip())
        
        # Convert to lowercase
        safe_name = safe_name.lower()
        
        # Create MD5 hash of list_source for uniqueness
        list_source_hash = hashlib.md5(list_source.encode('utf-8')).hexdigest()
        
        # Combine safe name with hash
        safe_filename = f"{safe_name}_{list_source_hash}"
        
        return safe_filename
