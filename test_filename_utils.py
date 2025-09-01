#!/usr/bin/env python3
"""
Simple test script to verify that FilenameUtils.create_safe_filename works correctly
"""

from FilenameUtils import FilenameUtils

def test_create_safe_filename():
    """Test various inputs to the create_safe_filename function"""
    
    test_cases = [
        ("Epic Systems Corporation", "epic_systems_corporation"),
        ("Cerner Corporation (Oracle)", "cerner_corporation_oracle"),
        ("athenahealth, Inc.", "athenahealth_inc"),
        ("NextGen Healthcare", "nextgen_healthcare"),
        ("Test-Company & Co.", "test_company_co"),
        ("Company with   multiple   spaces", "company_with_multiple_spaces"),
        ("123 Numeric Start", "123_numeric_start"),
        ("Special!@#$%^&*()Characters", "special_characters"),
    ]
    
    print("Testing FilenameUtils.create_safe_filename function:")
    print("=" * 60)
    
    all_passed = True
    for input_name, expected_output in test_cases:
        result = FilenameUtils.create_safe_filename(vendor_name=input_name)
        passed = result == expected_output
        status = "✓ PASS" if passed else "✗ FAIL"
        
        print(f"{status} Input: '{input_name}'")
        print(f"      Expected: '{expected_output}'")
        print(f"      Got:      '{result}'")
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
