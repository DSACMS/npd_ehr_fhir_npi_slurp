"""
Data validation utilities for FHIR processing
"""
import re
import phonenumbers
from phonenumbers import NumberParseException
from typing import Dict, Optional, Any
from pathlib import Path
import sys

# Add the parent directory to sys.path to import NPIValidator
sys.path.append(str(Path(__file__).parent.parent.parent))

try:
    from NPIValidator import NPIValidator as ExternalNPIValidator
    HAS_NPI_VALIDATOR = True
except ImportError:
    HAS_NPI_VALIDATOR = False
    ExternalNPIValidator = None


class NPIValidatorSingleton:
    """Singleton pattern for NPI validator to keep it in memory"""
    _instance = None
    _validator = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(NPIValidatorSingleton, cls).__new__(cls)
        return cls._instance
    
    def _initialize_validator(self):
        """Initialize the NPI validator once"""
        if not self._initialized:
            self._initialized = True
            if HAS_NPI_VALIDATOR and ExternalNPIValidator is not None:
                print("Initializing NPI validator (loading data into memory)...")
                try:
                    # Try to ensure builtin functions are available
                    import builtins
                    
                    # Temporarily patch the builtin namespace if needed
                    original_open = getattr(builtins, 'open', None)
                    if original_open is None:
                        # If open is missing, restore it
                        builtins.open = open
                        print("Restored 'open' builtin function")
                    
                    self._validator = ExternalNPIValidator()
                    print("✓ NPI validator initialized successfully")
                    
                    # Check if validator actually works
                    test_result = self._validator.is_this_npi_valid(npi_value="1234567890")
                    print(f"✓ NPI validator test completed (test result: {test_result})")
                    
                except ImportError as e:
                    print(f"Warning: Failed to import NPI validator: {e}")
                    self._validator = None
                except NameError as e:
                    print(f"Warning: Name error in NPI validator: {e}")
                    print("Continuing with format-only validation")
                    self._validator = None
                except AttributeError as e:
                    print(f"Warning: Attribute error in NPI validator: {e}")
                    print("Continuing with format-only validation")
                    self._validator = None
                except Exception as e:
                    print(f"Warning: Failed to initialize NPI validator: {e}")
                    print(f"Error type: {type(e).__name__}")
                    print("Continuing with format-only validation")
                    self._validator = None
            else:
                print("NPI validator not available (NPIValidator module not found)")
                print("Using format-only NPI validation")
                self._validator = None
    
    def get_validator(self):
        """Get the singleton NPI validator instance"""
        return self._validator
    
    def is_available(self) -> bool:
        """Check if NPI validator is available"""
        return self._validator is not None


# Global singleton instance - initialize it once
_npi_singleton = None

def get_npi_validator_singleton():
    """Get or create the NPI validator singleton"""
    global _npi_singleton
    if _npi_singleton is None:
        _npi_singleton = NPIValidatorSingleton()
        _npi_singleton._initialize_validator()
    return _npi_singleton


class DataValidator:
    """Handles data validation and quality checks"""
    
    def __init__(self, *, npi_validator=None):
        """Initialize with optional shared NPI validator instance"""
        if npi_validator is not None:
            self.npi_validator = npi_validator
        else:
            # Use singleton if no validator provided
            singleton = get_npi_validator_singleton()
            self.npi_validator = singleton.get_validator()
    
    @classmethod
    def create_with_shared_npi_validator(cls):
        """Create DataValidator with shared NPI validator singleton"""
        singleton = get_npi_validator_singleton()
        return cls(npi_validator=singleton.get_validator())
    
    @staticmethod
    def _is_valid_npi_format(npi_value: str) -> bool:
        """Basic NPI format validation"""
        return bool(re.match(r'^1\d{9}$', str(npi_value)))
    
    def validate_npi(self, *, npi_value: str, npi_system: str = "") -> Dict[str, Any]:
        """
        Validate NPI and return validation info
        
        Args:
            npi_value: The NPI value to validate
            npi_system: The system identifier (optional)
            
        Returns:
            Dictionary with validation results
        """
        if not npi_value:
            return {
                'npi_value': npi_value,
                'is_valid_format': False,
                'is_valid_api': False,
                'validation_error': 'Empty NPI value',
                'npi_system': npi_system
            }
        
        # Check format first
        is_valid_format = self._is_valid_npi_format(str(npi_value))
        
        if not is_valid_format:
            return {
                'npi_value': npi_value,
                'is_valid_format': False,
                'is_valid_api': False,
                'validation_error': 'Invalid NPI format (must be 10 digits starting with 1)',
                'npi_system': npi_system
            }
        
        # API validation if format is correct and validator is available
        if self.npi_validator is not None:
            try:
                is_valid_api = self.npi_validator.is_this_npi_valid(npi_value=str(npi_value))
                return {
                    'npi_value': npi_value,
                    'is_valid_format': True,
                    'is_valid_api': is_valid_api,
                    'validation_error': None if is_valid_api else 'NPI not found in registry',
                    'npi_system': npi_system
                }
            except Exception as e:
                return {
                    'npi_value': npi_value,
                    'is_valid_format': True,
                    'is_valid_api': False,
                    'validation_error': f"API validation failed: {str(e)}",
                    'npi_system': npi_system
                }
        else:
            # No API validator available, just return format validation
            return {
                'npi_value': npi_value,
                'is_valid_format': True,
                'is_valid_api': False,
                'validation_error': 'API validator not available',
                'npi_system': npi_system
            }
    
    @staticmethod
    def normalize_phone_number(phone_value: str) -> Dict[str, Any]:
        """
        Normalize phone number using phonenumbers library
        Returns dict with normalized number, extension, and validation info
        """
        if not phone_value:
            return {
                'original_value': phone_value,
                'normalized_number': '',
                'extension': '',
                'country_code': '',
                'is_valid': False,
                'parse_error': 'Empty phone number'
            }
        
        # Clean the input - remove common prefixes and formatting
        cleaned_value = str(phone_value).strip()
        
        # Try to extract extension first (common patterns)
        extension = ''
        extension_patterns = [
            r'\s*(?:ext\.?|extension|x)\s*(\d+)$',
            r'\s*#(\d+)$',
            r'\s*,\s*(\d+)$'
        ]
        
        for pattern in extension_patterns:
            match = re.search(pattern, cleaned_value, re.IGNORECASE)
            if match:
                extension = match.group(1)
                cleaned_value = re.sub(pattern, '', cleaned_value, flags=re.IGNORECASE).strip()
                break
        
        try:
            # Try parsing as US number first (most common case)
            try:
                parsed_number = phonenumbers.parse(cleaned_value, "US")
            except NumberParseException:
                # If US parsing fails, try without region
                parsed_number = phonenumbers.parse(cleaned_value, None)
            
            # Validate the parsed number
            is_valid = phonenumbers.is_valid_number(parsed_number)
            
            # Format the number in international format
            if is_valid:
                normalized_number = phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164)
                country_code = f"+{parsed_number.country_code}"
            else:
                normalized_number = phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164)
                country_code = f"+{parsed_number.country_code}" if hasattr(parsed_number, 'country_code') else ''
            
            return {
                'original_value': phone_value,
                'normalized_number': normalized_number,
                'extension': extension,
                'country_code': country_code,
                'is_valid': is_valid,
                'parse_error': None
            }
            
        except NumberParseException as e:
            return {
                'original_value': phone_value,
                'normalized_number': '',
                'extension': extension,
                'country_code': '',
                'is_valid': False,
                'parse_error': str(e)
            }
        except Exception as e:
            return {
                'original_value': phone_value,
                'normalized_number': '',
                'extension': extension,
                'country_code': '',
                'is_valid': False,
                'parse_error': f"Unexpected error: {str(e)}"
            }
    
    @staticmethod
    def validate_url(url: str) -> Dict[str, Any]:
        """Basic URL validation"""
        if not url:
            return {
                'url': url,
                'is_valid': False,
                'validation_error': 'Empty URL'
            }
        
        # Basic URL pattern check
        url_pattern = re.compile(
            r'^https?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        
        is_valid = bool(url_pattern.match(url))
        
        return {
            'url': url,
            'is_valid': is_valid,
            'validation_error': None if is_valid else 'Invalid URL format'
        }
    
    @staticmethod
    def validate_email(email: str) -> Dict[str, Any]:
        """Basic email validation"""
        if not email:
            return {
                'email': email,
                'is_valid': False,
                'validation_error': 'Empty email'
            }
        
        # Basic email pattern
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        is_valid = bool(email_pattern.match(email))
        
        return {
            'email': email,
            'is_valid': is_valid,
            'validation_error': None if is_valid else 'Invalid email format'
        }
