#!/usr/bin/env python3
"""
NPIValidator - A class to validate NPI numbers against the CMS NPI Registry

This class exists to validate NPI records as described in AI_Instructions/ValidateNPI.md

The class loads a cache of previously validated NPIs from ./prod_data/valid_npi_list.csv
and validates new NPIs against the CMS NPI Registry API, caching results for future use.
"""

import csv
import os
import re
import requests
import time
from pathlib import Path
from typing import Dict, Optional, Union


class NPIValidator:
    """
    A class to validate NPI numbers with caching support.
    
    Loads existing validation results from CSV cache and validates new NPIs
    against the CMS NPI Registry API. Updates cache with new results.
    """
    
    def __init__(self, *, cache_file_path: Optional[str] = None):
        """
        Initialize the NPIValidator with cache loading.
        
        Args:
            cache_file_path: Path to the CSV cache file. Defaults to ./prod_data/valid_npi_list.csv
        """
        if cache_file_path is None:
            self.cache_file_path = Path("./prod_data/valid_npi_list.csv")
        else:
            self.cache_file_path = Path(cache_file_path)
        
        # Internal cache - maps NPI to validity status
        self.npi_cache: Dict[str, bool] = {}
        
        # Track NPIs that were added during this session (need to be saved)
        self.newly_validated_npis: Dict[str, bool] = {}
        
        # Load existing cache
        self._load_cache()
    
    def _load_cache(self):
        """Load existing NPI validation results from CSV cache file."""
        if not self.cache_file_path.exists():
            print(f"Cache file not found: {self.cache_file_path}")
            print("Starting with empty cache.")
            return
        
        try:
            with open(self.cache_file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    npi = str(row.get('npi', '')).strip()
                    is_invalid = row.get('is_invalid', '').strip()
                    
                    if npi and is_invalid:
                        # Convert string values to boolean
                        # 'Invalid NPI' -> False, 'Valid NPI' -> True
                        is_valid = is_invalid == 'Valid NPI'
                        self.npi_cache[npi] = is_valid
            
            print(f"Loaded {len(self.npi_cache)} NPIs from cache")
            
        except Exception as e:
            print(f"Error loading cache file: {e}")
            print("Starting with empty cache.")
    
    def _save_cache(self):
        """Save all validation results back to CSV cache file."""
        if not self.newly_validated_npis:
            return  # No new data to save
        
        try:
            # Create directory if it doesn't exist
            self.cache_file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Combine existing and new data
            all_npis = {**self.npi_cache}
            
            # Write all data to CSV
            with open(self.cache_file_path, 'w', newline='', encoding='utf-8') as f:
                fieldnames = ['npi', 'is_invalid']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for npi, is_valid in all_npis.items():
                    is_invalid_str = 'Valid NPI' if is_valid else 'Invalid NPI'
                    writer.writerow({
                        'npi': npi,
                        'is_invalid': is_invalid_str
                    })
            
            print(f"Saved {len(self.newly_validated_npis)} new NPI validations to cache")
            
        except Exception as e:
            print(f"Error saving cache file: {e}")
    
    @staticmethod
    def _is_valid_npi_format(*, npi_value: str) -> bool:
        """
        Check if NPI has valid format (exactly 10 digits).
        
        Args:
            npi_value: The NPI value to check
            
        Returns:
            True if NPI format is valid, False otherwise
        """
        if not npi_value:
            return False
        # Remove any non-digit characters and check if it's exactly 10 digits
        digits_only = re.sub(r'\D', '', str(npi_value))
        return len(digits_only) == 10
    
    @staticmethod
    def _validate_npi_via_api(*, npi_value: str, max_retries: int = 3, delay: float = 0.1) -> Dict[str, Union[bool, str, int, None]]:
        """
        Validate NPI against the CMS NPI Registry API.
        
        Args:
            npi_value: The NPI value to validate
            max_retries: Maximum number of API call retries
            delay: Base delay between retries in seconds
            
        Returns:
            Dictionary with validation results including is_valid_api, api_error, etc.
        """
        if not NPIValidator._is_valid_npi_format(npi_value=npi_value):
            return {
                'is_valid_format': False,
                'is_valid_api': False,
                'api_error': 'Invalid NPI format',
                'result_count': 0
            }
        
        # Clean the NPI value to digits only
        clean_npi = re.sub(r'\D', '', str(npi_value))
        
        url = f"https://npiregistry.cms.hhs.gov/api/?version=2.1&number={clean_npi}"
        
        for attempt in range(max_retries):
            try:
                # Add a small delay to be respectful to the API
                if attempt > 0:
                    time.sleep(delay * (2 ** attempt))  # Exponential backoff
                
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                
                data = response.json()
                result_count = data.get('result_count', 0)
                
                return {
                    'is_valid_format': True,
                    'is_valid_api': result_count > 0,
                    'api_error': None,
                    'result_count': result_count
                }
                
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:  # Last attempt
                    return {
                        'is_valid_format': True,
                        'is_valid_api': False,
                        'api_error': f"API request failed: {str(e)}",
                        'result_count': 0
                    }
                # Continue to next attempt
                continue
            except Exception as e:
                return {
                    'is_valid_format': True,
                    'is_valid_api': False,
                    'api_error': f"Unexpected error: {str(e)}",
                    'result_count': 0
                }
        
        return {
            'is_valid_format': True,
            'is_valid_api': False,
            'api_error': "Max retries exceeded",
            'result_count': 0
        }
    
    def is_this_npi_valid(self, *, npi_value: str) -> bool:
        """
        Check if the given NPI is valid.
        
        First checks internal cache, then validates via API if not cached.
        Caches new results for future use.
        
        Args:
            npi_value: The NPI value to validate
            
        Returns:
            True if NPI is valid, False otherwise
        """
        if not npi_value:
            return False
        
        # Clean the NPI value to digits only for consistent cache keys
        clean_npi = re.sub(r'\D', '', str(npi_value))
        
        # Check if we already have this NPI in cache
        if clean_npi in self.npi_cache:
            return self.npi_cache[clean_npi]
        
        # Not in cache, validate via API
        print(f"Fall back to validating NPI via API: {clean_npi}")
        api_result = self._validate_npi_via_api(npi_value=clean_npi)
        
        # Extract validity from API result - ensure it's a boolean
        is_valid = bool(api_result.get('is_valid_api', False))
        
        # Cache the result
        self.npi_cache[clean_npi] = is_valid
        self.newly_validated_npis[clean_npi] = is_valid
        
        return is_valid
    
    def __del__(self):
        """
        Destructor that saves newly validated NPIs to cache file.
        """
        try:
            self._save_cache()
        except Exception as e:
            print(f"Error in NPIValidator destructor: {e}")
