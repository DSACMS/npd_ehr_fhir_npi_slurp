#!/usr/bin/env python3
"""
FHIR Bundle Parser

This script parses a FHIR Bundle JSON file and extracts individual entries
into separate JSON files, organized by resource type and named by resource ID.

Features:
* Process a single file with --input_file
* Process all JSON files in a directory with --input_dir
* Automatically creates subdirectories with the same name as the input file (without .json extension)
* Extracts individual FHIR Bundle entries into separate JSON files
* Detailed CSV-style error reporting for failed files and processing errors

"""

import json
import os
from pathlib import Path
import sys
import argparse
import glob
import csv
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from data_files.env
load_dotenv('data_files.env')

def parse_fhir_bundle(input_file, output_dir, error_tracker=None):
    """
    Parse a FHIR Bundle and extract individual entries to separate files.
    
    Args:
        input_file (str): Path to the input FHIR Bundle JSON file
        output_dir (str): Directory to save individual entry files
        error_tracker (list): List to track errors for CSV reporting
        
    Returns:
        tuple: (success_bool, error_details_list)
    """
    
    if error_tracker is None:
        error_tracker = []
    
    file_errors = []
    
    # Create output directory if it doesn't exist
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    print(f"Loading FHIR Bundle from: {input_file}")
    
    try:
        # Load the JSON file
        with open(input_file, 'r', encoding='utf-8') as f:
            bundle = json.load(f)
        
        # Verify it's a FHIR Bundle
        if bundle.get('resourceType') != 'Bundle':
            error_msg = f"File is not a FHIR Bundle. Resource type: {bundle.get('resourceType')}"
            print(f"Error: {error_msg}")
            error_detail = {
                'file_path': os.path.abspath(input_file),
                'error_type': 'InvalidResourceType',
                'error_message': error_msg,
                'timestamp': datetime.now().isoformat()
            }
            file_errors.append(error_detail)
            error_tracker.append(error_detail)
            return False, file_errors
        
        entries = bundle.get('entry', [])
        print(f"Found {len(entries)} entries in the bundle")
        
        # Statistics
        resource_counts = {}
        processed_count = 0
        error_count = 0
        
        # Process each entry
        for i, entry in enumerate(entries):
            try:
                resource = entry.get('resource', {})
                resource_type = resource.get('resourceType', 'Unknown')
                resource_id = resource.get('id', f'no_id_{i}')
                
                # Count resource types
                resource_counts[resource_type] = resource_counts.get(resource_type, 0) + 1
                
                # Create filename: entry_{resource_id}.json
                filename = f"entry_{resource_id}.json"
                filepath = output_path / filename
                
                # Save the individual entry (including both resource and fullUrl if present)
                entry_data = {
                    'resource': resource
                }
                
                # Include fullUrl if present
                if 'fullUrl' in entry:
                    entry_data['fullUrl'] = entry['fullUrl']
                
                # Write to file
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(entry_data, f, indent=2, ensure_ascii=False)
                
                processed_count += 1
                
                # Progress indicator
                if processed_count % 1000 == 0:
                    print(f"Processed {processed_count} entries...")
                    
            except Exception as e:
                error_msg = f"Error processing entry {i}: {str(e)}"
                print(error_msg)
                error_detail = {
                    'file_path': os.path.abspath(input_file),
                    'error_type': 'EntryProcessingError',
                    'error_message': f"Entry {i}: {str(e)}",
                    'timestamp': datetime.now().isoformat()
                }
                file_errors.append(error_detail)
                error_tracker.append(error_detail)
                error_count += 1
                continue
        
        # Print summary
        print(f"\nProcessing complete!")
        print(f"Total entries processed: {processed_count}")
        print(f"Errors encountered: {error_count}")
        print(f"Output directory: {output_dir}")
        
        print(f"\nResource type breakdown:")
        for resource_type, count in sorted(resource_counts.items()):
            print(f"  {resource_type}: {count}")
        
        return True, file_errors
        
    except FileNotFoundError:
        error_msg = f"Input file '{input_file}' not found"
        print(f"Error: {error_msg}")
        error_detail = {
            'file_path': os.path.abspath(input_file),
            'error_type': 'FileNotFoundError',
            'error_message': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        file_errors.append(error_detail)
        error_tracker.append(error_detail)
        return False, file_errors
    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON in input file: {str(e)}"
        print(f"Error: {error_msg}")
        error_detail = {
            'file_path': os.path.abspath(input_file),
            'error_type': 'JSONDecodeError',
            'error_message': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        file_errors.append(error_detail)
        error_tracker.append(error_detail)
        return False, file_errors
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        print(f"Error: {error_msg}")
        error_detail = {
            'file_path': os.path.abspath(input_file),
            'error_type': 'UnexpectedError',
            'error_message': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        file_errors.append(error_detail)
        error_tracker.append(error_detail)
        return False, file_errors

def process_single_file(input_file, error_tracker=None):
    """
    Process a single FHIR Bundle file and create a subdirectory for its entries.
    
    Args:
        input_file (str): Path to the input FHIR Bundle JSON file
        error_tracker (list): List to track errors for CSV reporting
    
    Returns:
        tuple: (success_bool, error_details_list)
    """
    if error_tracker is None:
        error_tracker = []
        
    input_path = Path(input_file)
    
    # Create output directory name by removing .json extension
    output_dir_name = input_path.stem
    output_dir = input_path.parent / output_dir_name
    
    print(f"\nProcessing: {input_file}")
    print(f"Output directory: {output_dir}")
    
    return parse_fhir_bundle(str(input_path), str(output_dir), error_tracker)

def write_error_report_csv(error_tracker, output_file="processing_errors.csv"):
    """
    Write error details to a CSV file.
    
    Args:
        error_tracker (list): List of error dictionaries
        output_file (str): Path to the output CSV file
    """
    if not error_tracker:
        print("No errors to report.")
        return
    
    print(f"\nWriting error report to: {output_file}")
    
    # CSV headers
    headers = ['file_path', 'error_type', 'error_message', 'timestamp']
    
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            
            for error in error_tracker:
                writer.writerow(error)
        
        print(f"Error report written successfully. Total errors: {len(error_tracker)}")
        
    except Exception as e:
        print(f"Error writing CSV report: {e}")

def print_error_summary_table(error_tracker):
    """
    Print a formatted table summary of errors to console.
    
    Args:
        error_tracker (list): List of error dictionaries
    """
    if not error_tracker:
        return
    
    # Calculate column widths dynamically based on content
    header_path_len = len("File Path")
    header_type_len = len("Error Type") 
    header_message_len = len("Error Message")
    
    # Find maximum width needed for each column
    max_path_len = header_path_len
    max_type_len = header_type_len
    max_message_len = header_message_len
    
    for error in error_tracker:
        file_path = error['file_path']
        error_type = error['error_type']
        error_message = error['error_message']
        
        max_path_len = max(max_path_len, len(file_path))
        max_type_len = max(max_type_len, len(error_type))
        max_message_len = max(max_message_len, len(error_message))
    
    # Set column widths with some padding
    error_path_len = max_path_len + 2
    error_type_len = max_type_len + 2
    error_message_len = max_message_len + 2
    
    # Calculate total table width
    total_width = error_path_len + error_type_len + error_message_len + 4  # +4 for spacing
    
    print("\nERROR SUMMARY:")
    print("=" * total_width)
    print(f"{'File Path':<{error_path_len}} {'Error Type':<{error_type_len}} {'Error Message':<{error_message_len}}")
    print("=" * total_width)
    
    for error in error_tracker:
        file_path = error['file_path']
        error_type = error['error_type']
        error_message = error['error_message']
        
        print(f"{file_path:<{error_path_len}} {error_type:<{error_type_len}} {error_message:<{error_message_len}}")
    
    print("=" * total_width)

def main():
    """Main function to run the parser."""
    
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description='Parse FHIR Bundle JSON files and extract individual entries into separate JSON files.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process a single file
  %(prog)s --input_file bundle.json
  
  # Process all JSON files in a directory
  %(prog)s --input_dir ./data/service_json/
        """
    )
    
    # Create mutually exclusive group for input options
    input_group = parser.add_mutually_exclusive_group(required=True)
    
    input_group.add_argument(
        '--input_file',
        type=str,
        help='Path to a single FHIR Bundle JSON file to process'
    )
    
    input_group.add_argument(
        '--input_dir',
        type=str,
        help='Directory containing FHIR Bundle JSON files to process'
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    print("FHIR Bundle Parser")
    print("=" * 50)
    
    successful_files = 0
    failed_files = 0
    total_files = 0
    
    # Initialize error tracker for CSV reporting
    all_errors = []
    
    if args.input_file:
        # Process single file
        input_file = args.input_file
        
        print(f"Mode: Single file processing")
        print(f"Input file: {input_file}")
        
        # Check if input file exists
        if not os.path.exists(input_file):
            error_detail = {
                'file_path': os.path.abspath(input_file),
                'error_type': 'FileNotFoundError',
                'error_message': f"Input file '{input_file}' does not exist",
                'timestamp': datetime.now().isoformat()
            }
            all_errors.append(error_detail)
            print(f"Error: Input file '{input_file}' does not exist")
            total_files = 1
            failed_files = 1
        elif not input_file.lower().endswith('.json'):
            error_detail = {
                'file_path': os.path.abspath(input_file),
                'error_type': 'InvalidFileType',
                'error_message': "Input file must be a JSON file",
                'timestamp': datetime.now().isoformat()
            }
            all_errors.append(error_detail)
            print(f"Error: Input file must be a JSON file")
            total_files = 1
            failed_files = 1
        else:
            total_files = 1
            success, file_errors = process_single_file(input_file, all_errors)
            if success:
                successful_files = 1
            else:
                failed_files = 1
            
    elif args.input_dir:
        # Process all JSON files in directory
        input_dir = args.input_dir
        
        print(f"Mode: Directory processing")
        print(f"Input directory: {input_dir}")
        
        # Check if input directory exists
        if not os.path.exists(input_dir):
            error_detail = {
                'file_path': os.path.abspath(input_dir),
                'error_type': 'DirectoryNotFoundError',
                'error_message': f"Input directory '{input_dir}' does not exist",
                'timestamp': datetime.now().isoformat()
            }
            all_errors.append(error_detail)
            print(f"Error: Input directory '{input_dir}' does not exist")
            # Generate error report and exit
            error_log_dir = os.getenv('ERROR_LOG_DIR', './logs')
            error_log_path = Path(error_log_dir)
            error_log_path.mkdir(parents=True, exist_ok=True)
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            error_csv_filename = error_log_path / f"Step30_processing_errors_{timestamp_str}.csv"
            write_error_report_csv(all_errors, str(error_csv_filename))
            print_error_summary_table(all_errors)
            sys.exit(1)
        
        if not os.path.isdir(input_dir):
            error_detail = {
                'file_path': os.path.abspath(input_dir),
                'error_type': 'NotADirectoryError',
                'error_message': f"'{input_dir}' is not a directory",
                'timestamp': datetime.now().isoformat()
            }
            all_errors.append(error_detail)
            print(f"Error: '{input_dir}' is not a directory")
            # Generate error report and exit
            error_log_dir = os.getenv('ERROR_LOG_DIR', './logs')
            error_log_path = Path(error_log_dir)
            error_log_path.mkdir(parents=True, exist_ok=True)
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            error_csv_filename = error_log_path / f"Step30_processing_errors_{timestamp_str}.csv"
            write_error_report_csv(all_errors, str(error_csv_filename))
            print_error_summary_table(all_errors)
            sys.exit(1)
        
        # Find all JSON files in the directory
        json_pattern = os.path.join(input_dir, "*.json")
        json_files = glob.glob(json_pattern)
        
        if not json_files:
            error_detail = {
                'file_path': os.path.abspath(input_dir),
                'error_type': 'NoFilesFoundError',
                'error_message': f"No JSON files found in directory: {input_dir}",
                'timestamp': datetime.now().isoformat()
            }
            all_errors.append(error_detail)
            print(f"No JSON files found in directory: {input_dir}")
            # Generate error report and exit
            error_log_dir = os.getenv('ERROR_LOG_DIR', './logs')
            error_log_path = Path(error_log_dir)
            error_log_path.mkdir(parents=True, exist_ok=True)
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            error_csv_filename = error_log_path / f"Step30_processing_errors_{timestamp_str}.csv"
            write_error_report_csv(all_errors, str(error_csv_filename))
            print_error_summary_table(all_errors)
            sys.exit(1)
        
        # Sort files for consistent processing order
        json_files.sort()
        total_files = len(json_files)
        
        print(f"Found {total_files} JSON files to process")
        print()
        
        # Process each JSON file
        for i, json_file in enumerate(json_files, 1):
            print(f"[{i}/{total_files}] Processing: {os.path.basename(json_file)}")
            
            success, file_errors = process_single_file(json_file, all_errors)
            if success:
                successful_files += 1
                print("✓ Success")
            else:
                failed_files += 1
                print("✗ Failed")
    
    # Print final summary
    print("\n" + "=" * 50)
    print("PROCESSING SUMMARY")
    print("=" * 50)
    print(f"Total files processed: {total_files}")
    print(f"Successful: {successful_files}")
    print(f"Failed: {failed_files}")
    
    # Generate error report if there were any errors
    if all_errors:
        print(f"Total errors encountered: {len(all_errors)}")
        
        # Get ERROR_LOG_DIR from environment variables
        error_log_dir = os.getenv('ERROR_LOG_DIR', './logs')  # Default to ./logs if not set
        error_log_path = Path(error_log_dir)
        
        # Create error log directory if it doesn't exist
        error_log_path.mkdir(parents=True, exist_ok=True)
        
        # Generate CSV error report with full path
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_csv_filename = error_log_path / f"Step30_processing_errors_{timestamp_str}.csv"
        write_error_report_csv(all_errors, str(error_csv_filename))
        
        # Print formatted error summary table
        print_error_summary_table(all_errors)
        
    if failed_files > 0:
        print(f"\nWarning: {failed_files} files failed to process")
        if all_errors:
            error_log_dir = os.getenv('ERROR_LOG_DIR', './logs')
            print(f"Detailed error information saved to: {error_log_dir}/Step30_processing_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        sys.exit(1)
    else:
        print("\nAll files processed successfully!")
        sys.exit(0)

if __name__ == "__main__":
    main()
