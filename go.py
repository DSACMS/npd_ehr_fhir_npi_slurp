#!/usr/bin/env python3

"""
EHR FHIR NPI Slurp Pipeline
Complete data processing pipeline for FHIR endpoint analysis

Python equivalent of go.sh
"""

import sys
import os
import subprocess
import time
import glob
from pathlib import Path


def load_env_file(*, env_file_path="data_files.env"):
    """Load environment variables from a .env file."""
    if not os.path.exists(env_file_path):
        print(f"Warning: Environment file '{env_file_path}' not found. Using defaults.")
        return
    
    with open(env_file_path, 'r') as f:
        for line in f:
            line = line.strip()
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue
            
            # Parse KEY=VALUE format
            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()
                # Only set if not already in environment and value is not empty
                if key and value and key not in os.environ:
                    os.environ[key] = value


def get_env_var(*, key, default_value):
    """Get environment variable with a default fallback."""
    return os.environ.get(key, default_value)


def check_virtual_env():
    """Check if virtual environment is activated."""
    if not os.environ.get('VIRTUAL_ENV'):
        print("Warning: Virtual environment not detected. Consider running:")
        print("source source_me_to_get_venv.sh")
        print("")


def run_step(*, step_num, description, command_args, success_message=None):
    """
    Run a pipeline step with proper error handling and logging.
    
    Args:
        step_num: Step number for display
        description: Description of what this step does
        command_args: List of command arguments to execute
        success_message: Optional custom success message
    """
    print(f"Step {step_num}: {description}...")
    
    try:
        # Run the command
        result = subprocess.run(command_args, check=True, capture_output=False)
        
        # Success message
        success_msg = success_message or f"✓ Step {step_num} completed"
        print(success_msg)
        print("")
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Step {step_num} failed with return code {e.returncode}")
        print(f"Command: {' '.join(command_args)}")
        sys.exit(1)
    except FileNotFoundError as e:
        print(f"❌ Step {step_num} failed: {e}")
        print(f"Command: {' '.join(command_args)}")
        sys.exit(1)


def compress_file(*, file_path):
    """
    Compress a big file using gzip if it exists.
    
    Args:
        file_path: Path to the JSON file to compress
    """
    if os.path.exists(file_path):
        try:
            with open(f"{file_path}.gz", "wb") as gz_file:
                result = subprocess.run([
                    "gzip", "-c", file_path
                ], stdout=gz_file, check=True)
            filename = os.path.basename(file_path)
            print(f"  ✓ {filename}.gz created")
        except subprocess.CalledProcessError as e:
            filename = os.path.basename(file_path)
            print(f"  ❌ Failed to compress {filename}: {e}")
    else:
        filename = os.path.basename(file_path)
        print(f"  ⚠ {filename} not found, skipping compression")


def main():
    """Main pipeline execution."""
    print("Starting EHR FHIR NPI Slurp Pipeline...")
    print("========================================")
    
    # Load environment variables from data_files.env
    load_env_file()
    
    # Check virtual environment
    check_virtual_env()


    # Step 1: Extract list sources from Lantern CSV
    run_step(
        step_num=1,
        description="Extracting list sources from Lantern CSV",
        command_args=[
            "python", "Step10_extract_list_source_from_lantern_csv.py",
            "--input_file", get_env_var(key="LANTERN_CSV_INPUT", default_value="local_data/lantern_csv/fhir_endpoints.csv"),
            "--output_file", get_env_var(key="LIST_SOURCES_SUMMARY", default_value="../npd_ehr_scrape_cache/list_sources_summary.csv")
        ]
    )
    
    # Step 2: Download service JSON files
    run_step(
        step_num=2,
        description="Downloading CEHRT JSON files",
        command_args=[
            "python", "Step20_download_list_source_json.py",
            "--input_file", get_env_var(key="LIST_SOURCES_SUMMARY", default_value="../npd_ehr_scrape_cache/list_sources_summary.csv"),
            "--output_dir", get_env_var(key="CEHRT_CACHE_DIR", default_value="../npd_ehr_scrape_cache/cache/fhir_json_cache/"),
            "--delay", get_env_var(key="DOWNLOAD_DELAY", default_value="1.0")
        ]
    )


    # Step 3: Parse FHIR bundles
    run_step(
        step_num=3,
        description="Parsing FHIR bundles",
        command_args=[
            "python", "Step30_parse_source_bundle.py",
            "--input_dir", get_env_var(key="SERVICE_JSON_DIR", default_value="../npd_ehr_scrape_cache/cehrt_fhir_json/")
        ]
    )

   
    # Step 4: Extract and normalize CSV data
    run_step(
        step_num=4,
        description="Extracting and normalizing CSV data",
        command_args=[
            "python", "Step40_extract_csv_data.py",
            "--input_dir", get_env_var(key="SERVICE_JSON_DIR", default_value="../npd_ehr_scrape_cache/cehrt_fhir_json/"),
            "--output_dir", get_env_var(key="NORMALIZED_CSV_DIR", default_value="../npd_ehr_scrape_cache/cache/summary_data/")
        ]
    )
    

    # Step 5: Clean and validate org_to_npi data
    print("Step 5: Cleaning and validating org_to_npi data...")
    print("  - Filtering for valid HTTPS URLs and 10-digit NPI numbers")
    print("  - Checking domain responsiveness")
    print("  - Outputting clean data for further processing")
    run_step(
        step_num=5,
        description="",  # Already printed above
        command_args=[
            "python", "Step50_simple_clean_output.py",
            "--input_file", get_env_var(key="ORG_TO_NPI_RAW", default_value="../npd_ehr_scrape_cache/cache/summary_data/step40_org_to_npi.csv"),
            "--output_file", get_env_var(key="CLEAN_NPI_TO_ORG_FHIR_URL", default_value="../npd_ehr_scrape_cache/cache/summary_data/step50_clean_npi_to_org_fhir_url.csv")
        ]
    )
    
   

    # Step 6: Discover FHIR endpoints at multiple directory levels
    print("Step 6: Discovering FHIR endpoints...")
    print("  - Testing multiple directory levels for each domain")
    print("  - Looking for: Capability Statement, SMART Config, OpenAPI, Swagger")
    print("  - Enriching data with endpoint discovery results")
    run_step(
        step_num=6,
        description="",  # Already printed above
        command_args=[
            "python", "Step60_CalculateOpenEndpoints.py",
            "--input_csv_file", get_env_var(key="CLEAN_NPI_TO_ORG_FHIR_URL", default_value="../npd_ehr_scrape_cache/cache/summary_data/step50_clean_npi_to_org_fhir_url.csv"),
            "--output_csv_file", get_env_var(key="ENRICHED_ENDPOINTS", default_value="../npd_ehr_scrape_cache/cache/summary_data/step60_enriched_endpoints.csv")
        ]
    )
    
    # Step 89: Generate CEHRT Dashboard CSV
    print("Step 89: Generating CEHRT compliance dashboard CSV...")
    print("  - Aggregating compliance results per vendor")
    print("  - Reading vendor mapping from list_sources_summary.csv")
    print("  - Combining endpoint discovery with partial compliance data")
    print("  - Creating dashboard input CSV")
    run_step(
        step_num=89,
        description="",  # Already printed above
        command_args=[
            "python", "Step89_GenerateCEHRTDashboardCSV.py",
            "--list_sources_path", get_env_var(key="LIST_SOURCES_SUMMARY", default_value="../npd_ehr_scrape_cache/list_sources_summary.csv"),
            "--enriched_endpoints_path", get_env_var(key="ENRICHED_ENDPOINTS", default_value="../npd_ehr_scrape_cache/cache/summary_data/step60_enriched_endpoints.csv"),
            "--org_to_npi_path", get_env_var(key="ORG_TO_NPI_RAW", default_value="../npd_ehr_scrape_cache/cache/summary_data/step40_org_to_npi.csv"),
            "--output_csv_path", get_env_var(key="CEHRT_FHIR_REPORT_CSV", default_value="../npd_ehr_scrape_cache/cache/summary_data/step89_CEHRT_FHIR_Report.csv")
        ]
    )
    
    # Step 90: Create CEHRT Dashboard Markdown Report
    print("Step 90: Creating CEHRT dashboard markdown report...")
    print("  - Converting CSV compliance data to visual dashboard")
    print("  - Using icons for pass/fail status visualization")
    print("  - Generating CEHRT_FHIR_Report.md")
    run_step(
        step_num=90,
        description="",  # Already printed above
        command_args=[
            "python", "Step90_MakeCEHRTDashboard.py",
            "--input_csv_path", get_env_var(key="CEHRT_FHIR_REPORT_CSV", default_value="../npd_ehr_scrape_cache/cache/summary_data/step89_CEHRT_FHIR_Report.csv"),
            "--output_md_path", get_env_var(key="CEHRT_FHIR_REPORT_MD", default_value="../npd_ehr_scrape_cache/cache/summary_data/step90_CEHRT_FHIR_Report.md")
        ]
    )

   

    # Final Step: Compress large JSON files for GitHub storage
    print("Final Step: Compressing large JSON files for GitHub storage...")
    
    # List of file patterns to compress for GitHub storage (supports glob patterns)
    file_patterns_to_compress = [
        "../npd_ehr_scrape_cache/cehrt_fhir_json/athenahealth_inc*.json",
        "../npd_ehr_scrape_cache/cehrt_fhir_json/epic_systems_corporation*.json",
        "../npd_ehr_scrape_cache/cache/summary_data/step40_org_to_npi.csv"
    ]
    
    # Expand glob patterns and compress each matching file
    for file_pattern in file_patterns_to_compress:
        # Use glob to find all files matching the pattern
        matching_files = glob.glob(file_pattern)
        
        if matching_files:
            # Sort files for consistent processing order
            matching_files.sort()
            for file_path in matching_files:
                filename = os.path.basename(file_path)
                print(f"  - Compressing {filename}")
                compress_file(file_path=file_path)
        else:
            # Handle case where no files match the pattern
            pattern_name = os.path.basename(file_pattern)
            print(f"  ⚠ No files found matching pattern: {pattern_name}")
    
    print("")

    # Success summary
    print("========================================")
    print("Pipeline completed successfully!")
    print("")
    print("Output files are available in:")
    print(f"  - {get_env_var(key='NORMALIZED_CSV_DIR', default_value='../npd_ehr_scrape_cache/cache/summary_data/')}/ (raw extracted data)")
    print(f"  - {get_env_var(key='CLEAN_NPI_TO_ORG_FHIR_URL', default_value='../npd_ehr_scrape_cache/cache/summary_data/step50_clean_npi_to_org_fhir_url.csv')} (cleaned org/NPI data)")
    print(f"  - {get_env_var(key='ENRICHED_ENDPOINTS', default_value='../npd_ehr_scrape_cache/cache/summary_data/step60_enriched_endpoints.csv')} (with FHIR endpoint discovery)")
    print(f"  - {get_env_var(key='CEHRT_FHIR_REPORT_CSV', default_value='../npd_ehr_scrape_cache/cache/summary_data/step89_CEHRT_FHIR_Report.csv')} (compliance summary by vendor)")
    print(f"  - {get_env_var(key='CEHRT_FHIR_REPORT_MD', default_value='../npd_ehr_scrape_cache/cache/summary_data/step90_CEHRT_FHIR_Report.md')} (visual compliance dashboard)")
    print("")
    print("Key deliverables:")
    print(f"  - {get_env_var(key='CEHRT_FHIR_REPORT_MD', default_value='../npd_ehr_scrape_cache/cache/summary_data/step90_CEHRT_FHIR_Report.md')}: Visual dashboard showing vendor compliance")
    print(f"  - {get_env_var(key='ENRICHED_ENDPOINTS', default_value='../npd_ehr_scrape_cache/cache/summary_data/step60_enriched_endpoints.csv')}: Complete dataset with endpoint discovery")
    print("")
    print("Additional options:")
    print(f"  - Run tests: python {get_env_var(key='TEST_PIPELINE', default_value='test_pipeline.py')}")
    print("  - Test mode (faster): python Step40_extract_csv_data.py --test")
    print(f"  - View dashboard: open {get_env_var(key='CEHRT_FHIR_REPORT_MD', default_value='../npd_ehr_scrape_cache/cache/summary_data/step90_CEHRT_FHIR_Report.md')}")


if __name__ == "__main__":
    main()
