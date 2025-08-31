#!/usr/bin/env python3
"""
Test script to verify the migration to separate cache repository is working
"""

import os
import sys
from pathlib import Path

# Import our functions
from go import load_env_file, get_env_var

def test_environment_variables():
    """Test that environment variables are loaded correctly"""
    print("Testing environment variable loading...")
    
    # Clear any existing environment variables to ensure clean test
    env_vars_to_clear = [
        'CEHRT_CACHE_DIR', 'LANTERN_CSV_INPUT', 'LIST_SOURCES_SUMMARY',
        'SERVICE_JSON_DIR', 'NORMALIZED_CSV_DIR', 'ORG_TO_NPI_RAW',
        'CLEAN_NPI_TO_ORG_FHIR_URL', 'ENRICHED_ENDPOINTS', 
        'CEHRT_FHIR_REPORT_CSV', 'CEHRT_FHIR_REPORT_MD'
    ]
    
    for var in env_vars_to_clear:
        if var in os.environ:
            del os.environ[var]
    
    # Load from our updated file
    load_env_file()
    
    # Test key variables that should point to cache repo
    expected_values = {
        'CEHRT_CACHE_DIR': '../npd_ehr_scrape_cache/cehrt_fhir_json/',
        'LIST_SOURCES_SUMMARY': '../npd_ehr_scrape_cache/list_sources_summary.csv',
        'NORMALIZED_CSV_DIR': '../npd_ehr_scrape_cache/cache/summary_data/',
        'ORG_TO_NPI_RAW': '../npd_ehr_scrape_cache/cache/summary_data/step40_org_to_npi.csv',
        'ENRICHED_ENDPOINTS': '../npd_ehr_scrape_cache/cache/summary_data/step60_enriched_endpoints.csv',
        'CEHRT_FHIR_REPORT_CSV': '../npd_ehr_scrape_cache/cache/summary_data/step89_CEHRT_FHIR_Report.csv',
        'CEHRT_FHIR_REPORT_MD': '../npd_ehr_scrape_cache/cache/summary_data/step90_CEHRT_FHIR_Report.md'
    }
    
    print("\nTesting cache repository paths:")
    all_passed = True
    
    for var_name, expected_value in expected_values.items():
        actual_value = get_env_var(key=var_name, default_value="NOT_SET")
        if actual_value == expected_value:
            print(f"✓ {var_name}: {actual_value}")
        else:
            print(f"❌ {var_name}: Expected '{expected_value}', got '{actual_value}'")
            all_passed = False
    
    return all_passed

def test_directory_structure():
    """Test that required directories exist"""
    print("\nTesting directory structure...")
    
    required_dirs = [
        '../npd_ehr_scrape_cache/cache/summary_data',
        '../npd_ehr_scrape_cache/cehrt_fhir_json'
    ]
    
    all_exist = True
    for dir_path in required_dirs:
        if Path(dir_path).exists():
            print(f"✓ Directory exists: {dir_path}")
        else:
            print(f"❌ Directory missing: {dir_path}")
            all_exist = False
    
    return all_exist

def test_step_prefixes():
    """Test that step-prefixed filenames are configured correctly"""
    print("\nTesting step-prefixed filenames...")
    
    filename_tests = [
        ('ORG_TO_NPI_RAW', 'step40_org_to_npi.csv'),
        ('ENRICHED_ENDPOINTS', 'step60_enriched_endpoints.csv'),
        ('CEHRT_FHIR_REPORT_CSV', 'step89_CEHRT_FHIR_Report.csv'),
        ('CEHRT_FHIR_REPORT_MD', 'step90_CEHRT_FHIR_Report.md')
    ]
    
    all_passed = True
    for var_name, expected_filename in filename_tests:
        full_path = get_env_var(key=var_name, default_value="NOT_SET")
        if expected_filename in full_path:
            print(f"✓ {var_name} contains '{expected_filename}': {full_path}")
        else:
            print(f"❌ {var_name} should contain '{expected_filename}': {full_path}")
            all_passed = False
    
    return all_passed

def main():
    print("=" * 60)
    print("MIGRATION TEST: Separate Cache Repository")
    print("=" * 60)
    
    # Run all tests
    env_test = test_environment_variables()
    dir_test = test_directory_structure()
    prefix_test = test_step_prefixes()
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY:")
    print(f"Environment Variables: {'✓ PASS' if env_test else '❌ FAIL'}")
    print(f"Directory Structure: {'✓ PASS' if dir_test else '❌ FAIL'}")
    print(f"Step Prefixes: {'✓ PASS' if prefix_test else '❌ FAIL'}")
    
    overall_success = env_test and dir_test and prefix_test
    
    if overall_success:
        print("\n🎉 MIGRATION TEST: ALL TESTS PASSED!")
        print("The migration to separate cache repository is working correctly.")
    else:
        print("\n❌ MIGRATION TEST: SOME TESTS FAILED")
        print("Please review the errors above and fix them.")
    
    return 0 if overall_success else 1

if __name__ == "__main__":
    sys.exit(main())
