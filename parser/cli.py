"""
Command-line interface for FHIR Cache Processor
"""
import argparse
import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional

from .processor import FHIRCacheProcessor


def create_argument_parser():
    """Create command-line argument parser"""
    parser = argparse.ArgumentParser(
        description='Process FHIR cache data and generate PostgreSQL-ready CSV files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process entire cache
  python -m parser.cli --cache-dir ../npd_ehr_scrape_cache/cache/fhir_json_cache --output-dir ./csv_output
  
  # Test mode (limited processing)
  python -m parser.cli --cache-dir ../npd_ehr_scrape_cache/cache/fhir_json_cache --output-dir ./csv_output --test
  
  # Specify custom report file
  python -m parser.cli --cache-dir ./test_cache --output-dir ./csv_output --report-file ./processing_report.json
        """
    )
    
    parser.add_argument(
        '--cache-dir',
        type=str,
        required=True,
        help='Path to the FHIR cache directory containing vendor subdirectories'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        required=True,
        help='Directory to save CSV files and reports'
    )
    
    parser.add_argument(
        '--test',
        action='store_true',
        help='Test mode: process only first 100 files per vendor for validation'
    )
    
    parser.add_argument(
        '--report-file',
        type=str,
        default=None,
        help='Path to save processing report JSON (default: processing_report_TIMESTAMP.json in output dir)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )
    
    return parser


def validate_arguments(*, cache_dir: str, output_dir: str) -> tuple:
    """Validate command-line arguments"""
    # Validate cache directory
    cache_path = Path(cache_dir)
    if not cache_path.exists():
        print(f"Error: Cache directory does not exist: {cache_dir}", file=sys.stderr)
        sys.exit(1)
    
    if not cache_path.is_dir():
        print(f"Error: Cache path is not a directory: {cache_dir}", file=sys.stderr)
        sys.exit(1)
    
    # Check if cache directory has vendor subdirectories
    vendor_dirs = [d for d in cache_path.iterdir() 
                  if d.is_dir() and 
                  (d / 'endpoint').exists() and 
                  (d / 'organization').exists()]
    
    if not vendor_dirs:
        print(f"Warning: No vendor directories found in cache directory: {cache_dir}")
        print("Expected structure: cache_dir/vendor_name/endpoint/ and cache_dir/vendor_name/organization/")
    
    # Validate/create output directory
    output_path = Path(output_dir)
    try:
        output_path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"Error: Cannot create output directory {output_dir}: {e}", file=sys.stderr)
        sys.exit(1)
    
    return cache_path, output_path


def generate_report_filename(*, output_dir: Path, custom_filename: Optional[str] = None) -> Path:
    """Generate report filename"""
    if custom_filename:
        report_path = Path(custom_filename)
        if not report_path.is_absolute():
            report_path = output_dir / report_path
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = output_dir / f"processing_report_{timestamp}.json"
    
    return report_path


def save_processing_report(*, report_data: dict, report_file: Path):
    """Save processing report to JSON file"""
    try:
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)
        print(f"\nProcessing report saved to: {report_file}")
    except Exception as e:
        print(f"Warning: Could not save processing report to {report_file}: {e}")


def print_summary(*, report_data: dict):
    """Print processing summary to console"""
    print("\n" + "="*60)
    print("PROCESSING SUMMARY")
    print("="*60)
    
    summary = report_data.get('processing_summary', {})
    resource_counts = report_data.get('resource_counts', {})
    table_stats = report_data.get('table_statistics', {})
    
    print(f"Run ID: {summary.get('run_id', 'Unknown')}")
    print(f"Processing Time: {summary.get('start_time', 'Unknown')} to {summary.get('end_time', 'Unknown')}")
    print(f"Success Rate: {summary.get('success_rate', 0):.1f}%")
    
    print(f"\nFiles Processed:")
    print(f"  Total: {summary.get('total_files', 0)}")
    print(f"  Successful: {summary.get('files_processed', 0)}")
    print(f"  Failed: {summary.get('files_failed', 0)}")
    
    print(f"\nResources by Type:")
    for resource_type, count in resource_counts.items():
        print(f"  {resource_type}: {count}")
    
    print(f"\nDatabase Tables Generated:")
    for table_name, count in table_stats.items():
        if count > 0:  # Only show tables with data
            print(f"  {table_name}: {count} records")
    
    print(f"\nVendors Processed: {summary.get('vendors_processed', 0)}")
    
    # Field coverage summary
    vendor_coverage = summary.get('vendor_coverage', {})
    if vendor_coverage:
        total_coverage = sum(v.get('coverage_percentage', 0) for v in vendor_coverage.values())
        avg_coverage = total_coverage / len(vendor_coverage) if vendor_coverage else 0
        print(f"Average Field Coverage: {avg_coverage:.1f}%")


def main():
    """Main CLI entry point"""
    parser = create_argument_parser()
    args = parser.parse_args()
    
    print("FHIR Cache Processor")
    print("=" * 50)
    
    # Validate arguments
    cache_path, output_path = validate_arguments(
        cache_dir=args.cache_dir,
        output_dir=args.output_dir
    )
    
    # Generate report filename
    report_path = generate_report_filename(
        output_dir=output_path,
        custom_filename=args.report_file
    )
    
    print(f"Cache Directory: {cache_path}")
    print(f"Output Directory: {output_path}")
    print(f"Report File: {report_path}")
    if args.test:
        print("Mode: TEST (limited processing)")
    else:
        print("Mode: FULL processing")
    
    try:
        # Initialize processor
        processor = FHIRCacheProcessor(
            cache_root=cache_path,
            output_dir=output_path
        )
        
        # Process the cache
        report_data = processor.process_entire_cache(test_mode=args.test)
        
        # Save report
        save_processing_report(report_data=report_data, report_file=report_path)
        
        # Print summary
        print_summary(report_data=report_data)
        
        print("\n" + "="*60)
        print("SUCCESS: Processing completed successfully!")
        print("="*60)
        
        # Check for high error rates
        summary = report_data.get('processing_summary', {})
        success_rate = summary.get('success_rate', 0)
        if success_rate < 50:
            print(f"WARNING: Low success rate ({success_rate:.1f}%). Check error logs.")
            sys.exit(1)
        
        sys.exit(0)
        
    except KeyboardInterrupt:
        print("\n\nProcessing interrupted by user")
        sys.exit(1)
        
    except Exception as e:
        print(f"\nERROR: Processing failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
