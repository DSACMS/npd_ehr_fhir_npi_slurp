#!/usr/bin/env python3
"""
Simple test script to verify that FilenameUtils.create_safe_filename works correctly
"""

from FilenameUtils import FilenameUtils

def test_create_safe_filename():
    """Test various inputs to the create_safe_filename function"""
    
    test_cases = [
        ("Epic Systems Corporation", "https://example.com/epic", "epic_systems_corporation"),
        ("Cerner Corporation (Oracle)", "https://example.com/cerner", "cerner_corporation_oracle"),
        ("athenahealth, Inc.", "https://example.com/athena", "athenahealth_inc"),
        ("NextGen Healthcare", "https://example.com/nextgen", "nextgen_healthcare"),
        ("Test-Company & Co.", "https://example.com/test", "test_company_co"),
        ("Company with   multiple   spaces", "https://example.com/spaces", "company_with_multiple_spaces"),
        ("123 Numeric Start", "https://example.com/numeric", "123_numeric_start"),
        ("Special!@#$%^&*()Characters", "https://example.com/special", "special_characters"),
    ]
    
    print("Testing FilenameUtils.create_safe_filename function:")
    print("=" * 60)
    
    all_passed = True
    for input_name, list_source, expected_base in test_cases:
        result = FilenameUtils.create_safe_filename(vendor_name=input_name, list_source=list_source)
        
        # The result should start with the expected base and end with a hash
        # We'll test that the base part matches and that there's a hash appended
        if result.startswith(expected_base + "_") and len(result) == len(expected_base) + 1 + 32:  # 32 chars for MD5
            passed = True
        else:
            passed = False
            
        status = "✓ PASS" if passed else "✗ FAIL"
        
        print(f"{status} Input: '{input_name}' + '{list_source}'")
        print(f"      Expected pattern: '{expected_base}_[32-char-hash]'")
        print(f"      Got:              '{result}'")
        print()
        
        if not passed:
            all_passed = False
    
    if all_passed:
        print("All tests passed! ✓")
        return True
    else:
        print("Some tests failed! ✗")
        return False

if __name__ == "__main__":
    test_create_safe_filename()
