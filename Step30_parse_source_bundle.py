#!/usr/bin/env python3
"""
FHIR Bundle Parser

This script parses a FHIR Bundle JSON file and extracts individual entries
into separate JSON files, organized by resource type and named by resource ID.

TODO: 
* Implement arguements of --input_dir and --input_file. If --input_dir is set then loop over every file in the directory and repeat the process on each one of them
* automatically create a subdirectory in the same directory the processed file is in with the same name as the file without the json file. 
* write all of that jsons seperated entry sub-files into that directory

"""

import json
import os
from pathlib import Path
import sys
import argparse

def parse_fhir_bundle(input_file, output_dir):
    """
    Parse a FHIR Bundle and extract individual entries to separate files.
    
    Args:
        input_file (str): Path to the input FHIR Bundle JSON file
        output_dir (str): Directory to save individual entry files
    """
    
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
            print(f"Error: File is not a FHIR Bundle. Resource type: {bundle.get('resourceType')}")
            return False
        
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
                print(f"Error processing entry {i}: {e}")
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
        
        return True
        
    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found")
        return False
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in input file: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False

def main():
    """Main function to run the parser."""
    
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description='Parse a FHIR Bundle JSON file and extract individual entries into separate JSON files.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --input bundle.json --output ./output_dir
  %(prog)s -i bundle.json -o ./output_dir
        """
    )
    
    parser.add_argument(
        '--input', '-i',
        type=str,
        required=True,
        help='Path to the input FHIR Bundle JSON file'
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        required=True,
        help='Directory to save individual entry files'
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    input_file = args.input
    output_dir = args.output
    
    print("FHIR Bundle Parser")
    print("=" * 50)
    print(f"Input file: {input_file}")
    print(f"Output directory: {output_dir}")
    print()
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' does not exist")
        sys.exit(1)
    
    # Parse the bundle
    success = parse_fhir_bundle(input_file, output_dir)
    
    if success:
        print("\nParsing completed successfully!")
        sys.exit(0)
    else:
        print("\nParsing failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
