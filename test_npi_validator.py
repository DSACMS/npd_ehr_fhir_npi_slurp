#!/usr/bin/env python3
"""
Simple test script for NPIValidator
"""

from NPIValidator import NPIValidator

def test_npi_validator():
    """Test the NPIValidator class"""
    
    # Initialize validator
    validator = NPIValidator()
    
    # Test with some example NPIs
    test_npis = [
        "1234567890",  # Valid format, but likely not a real NPI
        "123456789",   # Invalid format (9 digits)
        "12345678901", # Invalid format (11 digits)
        "1234567890abcd", # Invalid format (contains letters)
        "1568495397",  # Real NPI (Dr. John Smith example)
        ""             # Empty string
    ]
    
    print("Testing NPIValidator:")
    print("-" * 50)
    
    for npi in test_npis:
        try:
            result = validator.is_this_npi_valid(npi_value=npi)
            print(f"NPI: '{npi}' -> Valid: {result}")
        except Exception as e:
            print(f"NPI: '{npi}' -> Error: {e}")
    
    print("\nTest completed!")

if __name__ == "__main__":
    test_npi_validator()
