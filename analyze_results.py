#!/usr/bin/env python3
"""
Data Analysis and Validation Script for EHR FHIR NPI Slurp Results

This script provides analysis and validation of the processed CSV data,
generating summary reports and identifying data quality issues.
"""

import pandas as pd
import argparse
import sys
from pathlib import Path
import json

def load_csv_safely(file_path):
    """Load CSV file with error handling"""
    try:
        return pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"Warning: File not found: {file_path}")
        return None
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return None

def analyze_organizations(csv_dir):
    """Analyze organization data quality and completeness"""
    print("Analyzing Organizations...")
    print("=" * 50)
    
    orgs_df = load_csv_safely(csv_dir / 'distinct_organizations.csv')
    if orgs_df is None:
        return
    
    print(f"Total Organizations: {len(orgs_df)}")
    print(f"Active Organizations: {len(orgs_df[orgs_df['active'] == True])}")
    print(f"Inactive Organizations: {len(orgs_df[orgs_df['active'] == False])}")
    print()
    
    # Vendor analysis
    vendor_counts = orgs_df['vendor_name'].value_counts()
    print("Top 10 EHR Vendors by Organization Count:")
    print(vendor_counts.head(10))
    print()
    
    # Data completeness analysis
    print("Data Completeness Analysis:")
    print(f"Organizations with NPIs: {len(orgs_df[orgs_df['npi_count'] > 0])}")
    print(f"Organizations with Endpoints: {len(orgs_df[orgs_df['endpoint_count'] > 0])}")
    print(f"Organizations with Addresses: {len(orgs_df[orgs_df['address_count'] > 0])}")
    print(f"Organizations with Phone Numbers: {len(orgs_df[orgs_df['phone_count'] > 0])}")
    print()
    
    # Quality metrics
    avg_npis = orgs_df['npi_count'].mean()
    avg_endpoints = orgs_df['endpoint_count'].mean()
    print(f"Average NPIs per Organization: {avg_npis:.2f}")
    print(f"Average Endpoints per Organization: {avg_endpoints:.2f}")
    print()

def analyze_npis(csv_dir):
    """Analyze NPI validation results"""
    print("Analyzing NPIs...")
    print("=" * 50)
    
    npi_df = load_csv_safely(csv_dir / 'org_to_npi.csv')
    if npi_df is None:
        return
    
    print(f"Total NPI Records: {len(npi_df)}")
    print(f"Valid NPIs: {len(npi_df[npi_df['is_invalid_npi'] == 0])}")
    print(f"Invalid NPIs: {len(npi_df[npi_df['is_invalid_npi'] == 1])}")
    
    if 'result_count' in npi_df.columns:
        validated_npis = len(npi_df[npi_df['result_count'] > 0])
        print(f"API Validated NPIs: {validated_npis}")
    
    print()
    
    # NPI system analysis
    if 'npi_system' in npi_df.columns:
        system_counts = npi_df['npi_system'].value_counts()
        print("NPI Systems Used:")
        print(system_counts)
        print()

def analyze_addresses(csv_dir):
    """Analyze address data quality"""
    print("Analyzing Addresses...")
    print("=" * 50)
    
    addr_df = load_csv_safely(csv_dir / 'distinct_addresses.csv')
    if addr_df is None:
        return
    
    print(f"Total Unique Addresses: {len(addr_df)}")
    
    # State analysis
    if 'state' in addr_df.columns:
        state_counts = addr_df['state'].value_counts()
        print(f"Addresses by State (Top 10):")
        print(state_counts.head(10))
        print()
    
    # Address completeness
    complete_addresses = addr_df[
        (addr_df['address_line1'].notna()) & 
        (addr_df['city'].notna()) & 
        (addr_df['state'].notna()) & 
        (addr_df['postal_code'].notna())
    ]
    print(f"Complete Addresses (all fields): {len(complete_addresses)}")
    print(f"Address Completeness Rate: {len(complete_addresses)/len(addr_df)*100:.1f}%")
    print()

def analyze_phones(csv_dir):
    """Analyze phone number validation results"""
    print("Analyzing Phone Numbers...")
    print("=" * 50)
    
    phone_df = load_csv_safely(csv_dir / 'distinct_phones.csv')
    if phone_df is None:
        return
    
    print(f"Total Unique Phone Numbers: {len(phone_df)}")
    
    if 'is_valid' in phone_df.columns:
        valid_phones = len(phone_df[phone_df['is_valid'] == True])
        print(f"Valid Phone Numbers: {valid_phones}")
        print(f"Phone Validation Rate: {valid_phones/len(phone_df)*100:.1f}%")
    
    if 'country_code' in phone_df.columns:
        country_counts = phone_df['country_code'].value_counts()
        print("Phone Numbers by Country Code:")
        print(country_counts.head(10))
    
    print()

def analyze_errors(csv_dir):
    """Analyze processing errors"""
    print("Analyzing Processing Errors...")
    print("=" * 50)
    
    error_df = load_csv_safely(csv_dir / 'processing_errors.csv')
    if error_df is None:
        print("No error file found or no errors occurred.")
        return
    
    print(f"Total Processing Errors: {len(error_df)}")
    
    if len(error_df) > 0:
        # Error type analysis
        error_types = error_df['error'].value_counts()
        print("Most Common Error Types:")
        print(error_types.head(10))
        print()
        
        # Show sample errors
        print("Sample Error Messages:")
        for i, error in enumerate(error_df['error'].head(5)):
            print(f"{i+1}. {error}")
        print()

def generate_summary_report(csv_dir, output_file=None):
    """Generate a comprehensive summary report"""
    print("Generating Summary Report...")
    print("=" * 50)
    
    # Load all data files
    orgs_df = load_csv_safely(csv_dir / 'distinct_organizations.csv')
    npi_df = load_csv_safely(csv_dir / 'org_to_npi.csv')
    addr_df = load_csv_safely(csv_dir / 'distinct_addresses.csv')
    phone_df = load_csv_safely(csv_dir / 'distinct_phones.csv')
    endpoint_df = load_csv_safely(csv_dir / 'distinct_endpoints.csv')
    error_df = load_csv_safely(csv_dir / 'processing_errors.csv')
    
    # Create summary statistics
    summary = {
        "processing_summary": {
            "total_organizations": len(orgs_df) if orgs_df is not None else 0,
            "total_npis": len(npi_df) if npi_df is not None else 0,
            "total_addresses": len(addr_df) if addr_df is not None else 0,
            "total_phone_numbers": len(phone_df) if phone_df is not None else 0,
            "total_endpoints": len(endpoint_df) if endpoint_df is not None else 0,
            "total_errors": len(error_df) if error_df is not None else 0
        }
    }
    
    # Add organization analysis
    if orgs_df is not None:
        summary["organization_analysis"] = {
            "active_organizations": int(len(orgs_df[orgs_df['active'] == True])),
            "vendor_count": int(orgs_df['vendor_name'].nunique()),
            "top_vendors": orgs_df['vendor_name'].value_counts().head(5).to_dict(),
            "avg_npis_per_org": float(orgs_df['npi_count'].mean()),
            "avg_endpoints_per_org": float(orgs_df['endpoint_count'].mean())
        }
    
    # Add NPI analysis
    if npi_df is not None:
        summary["npi_analysis"] = {
            "valid_npis": int(len(npi_df[npi_df['is_invalid_npi'] == 0])),
            "invalid_npis": int(len(npi_df[npi_df['is_invalid_npi'] == 1])),
            "validation_rate": float(len(npi_df[npi_df['is_invalid_npi'] == 0]) / len(npi_df) * 100)
        }
        
        if 'result_count' in npi_df.columns:
            summary["npi_analysis"]["api_validated"] = int(len(npi_df[npi_df['result_count'] > 0]))
    
    # Add address analysis
    if addr_df is not None:
        complete_addresses = addr_df[
            (addr_df['address_line1'].notna()) & 
            (addr_df['city'].notna()) & 
            (addr_df['state'].notna()) & 
            (addr_df['postal_code'].notna())
        ]
        summary["address_analysis"] = {
            "complete_addresses": int(len(complete_addresses)),
            "completeness_rate": float(len(complete_addresses) / len(addr_df) * 100),
            "states_represented": int(addr_df['state'].nunique()) if 'state' in addr_df.columns else 0
        }
    
    # Add phone analysis
    if phone_df is not None and 'is_valid' in phone_df.columns:
        summary["phone_analysis"] = {
            "valid_phones": int(len(phone_df[phone_df['is_valid'] == True])),
            "phone_validation_rate": float(len(phone_df[phone_df['is_valid'] == True]) / len(phone_df) * 100)
        }
    
    # Print summary
    print(json.dumps(summary, indent=2))
    
    # Save to file if requested
    if output_file:
        with open(output_file, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"\nSummary report saved to: {output_file}")
    
    return summary

def main():
    parser = argparse.ArgumentParser(description='Analyze EHR FHIR NPI Slurp results')
    parser.add_argument('--csv_dir', default='./data/normalized_csv_files',
                       help='Directory containing CSV result files')
    parser.add_argument('--summary_only', action='store_true',
                       help='Generate only summary report')
    parser.add_argument('--output_file', 
                       help='Save summary report to JSON file')
    
    args = parser.parse_args()
    
    csv_dir = Path(args.csv_dir)
    
    if not csv_dir.exists():
        print(f"Error: CSV directory does not exist: {csv_dir}")
        sys.exit(1)
    
    print("EHR FHIR NPI Slurp - Data Analysis Report")
    print("=" * 60)
    print(f"Analyzing data from: {csv_dir}")
    print()
    
    if args.summary_only:
        generate_summary_report(csv_dir, args.output_file)
    else:
        # Run all analyses
        analyze_organizations(csv_dir)
        analyze_npis(csv_dir)
        analyze_addresses(csv_dir)
        analyze_phones(csv_dir)
        analyze_errors(csv_dir)
        generate_summary_report(csv_dir, args.output_file)

if __name__ == "__main__":
    main()
