#!/usr/bin/env python3

"""
JSON Data Mine Analysis Runner

Runs all looper classes in parallel using subprocess calls to maximize performance.
Can run in test mode or full mode, and optionally generate markdown reports.
"""

import sys
import os
import argparse
import time
import subprocess
import concurrent.futures
import re
from typing import List, Dict, Tuple
from pathlib import Path


class AnalysisRunner:
    """
    Coordinates running all JSON data mine analysis loopers in parallel.
    """
    
    def __init__(self):
        """Initialize the analysis runner with all available loopers."""
        # Define all looper scripts in logical order
        self.looper_configs = [
            {
                'script': 'ResourceTypeLooper.py',
                'name': 'Resource Type Analysis',
                'description': 'Analyzes FHIR resource types and their distribution'
            },
            {
                'script': 'EndpointIDLooper.py',
                'name': 'Endpoint ID Analysis',
                'description': 'Categorizes endpoint ID patterns using regex classification'
            },
            {
                'script': 'ActiveStatusLooper.py',
                'name': 'Active Status Analysis',
                'description': 'Analyzes active status field presence and values'
            },
            {
                'script': 'TelecomEmailLooper.py',
                'name': 'Telecom Email Analysis',
                'description': 'Validates and analyzes email telecom entries'
            },
            {
                'script': 'TelecomPhoneLooper.py',
                'name': 'Telecom Phone Analysis',
                'description': 'Validates and analyzes phone telecom entries'
            },
            {
                'script': 'NPILooper.py',
                'name': 'NPI Identifier Analysis',
                'description': 'Analyzes NPI identifiers for format and validity'
            },
            {
                'script': 'AddressLooper.py',
                'name': 'Address Component Analysis',
                'description': 'Analyzes address field components and their presence'
            },
            {
                'script': 'AddressFieldLooper.py',
                'name': 'Address Field Classification',
                'description': 'Classifies address field contents using regex patterns'
            },
            {
                'script': 'ConnectionTypeLooper.py',
                'name': 'Connection Type Analysis',
                'description': 'Analyzes connectionType fields and their system/code values'
            },
            {
                'script': 'MetaLooper.py',
                'name': 'Meta Tag Analysis',
                'description': 'Analyzes meta tag structure and subfield presence'
            },
            {
                'script': 'NameLooper.py',
                'name': 'Name Field Analysis',
                'description': 'Analyzes presence of name fields in resources'
            },
            {
                'script': 'PayloadTypeLooper.py',
                'name': 'Payload Type Analysis',
                'description': 'Analyzes payloadType fields including coding and address classification'
            }
        ]
    
    def _run_single_analysis(self, *, config: Dict, test_mode: bool, 
                           generate_reports: bool, report_directory: str) -> Dict:
        """
        Run a single analysis using subprocess.
        
        Args:
            config: Analysis configuration dictionary
            test_mode: If True, run in test mode
            generate_reports: If True, generate markdown reports
            report_directory: Directory to save reports
            
        Returns:
            Dictionary with analysis results
        """
        script = config['script']
        name = config['name']
        
        # Build command
        cmd = ['python', script]
        if test_mode:
            cmd.append('--test-mode')
        
        # Run the subprocess
        start_time = time.time()
        try:
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=300,  # 5 minute timeout
                cwd=os.getcwd()
            )
            
            execution_time = time.time() - start_time
            
            if result.returncode == 0:
                # Parse output to extract statistics
                output = result.stdout
                processed_count = self._extract_processed_count(output)
                failure_count = self._extract_failure_count(output)
                
                # Generate report if requested and successful
                if generate_reports:
                    self._save_report_from_output(
                        output, script, report_directory
                    )
                
                return {
                    'name': name,
                    'script': script,
                    'processed': processed_count,
                    'failures': failure_count,
                    'time': execution_time,
                    'success': True,
                    'output': output
                }
            else:
                return {
                    'name': name,
                    'script': script,
                    'processed': 0,
                    'failures': 0,
                    'time': execution_time,
                    'success': False,
                    'error': f"Exit code {result.returncode}",
                    'stderr': result.stderr
                }
                
        except subprocess.TimeoutExpired:
            execution_time = time.time() - start_time
            return {
                'name': name,
                'script': script,
                'processed': 0,
                'failures': 0,
                'time': execution_time,
                'success': False,
                'error': 'Timeout (5 minutes)'
            }
        except Exception as e:
            execution_time = time.time() - start_time
            return {
                'name': name,
                'script': script,
                'processed': 0,
                'failures': 0,
                'time': execution_time,
                'success': False,
                'error': str(e)
            }
    
    def _extract_processed_count(self, output: str) -> int:
        """Extract processed file count from output."""
        match = re.search(r'\*\*Files Processed:\*\* (\d+)', output)
        if match:
            return int(match.group(1))
        
        # Fallback pattern
        match = re.search(r'(\d+) files processed', output)
        if match:
            return int(match.group(1))
        
        return 0
    
    def _extract_failure_count(self, output: str) -> int:
        """Extract failure count from output."""
        match = re.search(r'\*\*Files Failed:\*\* (\d+)', output)
        if match:
            return int(match.group(1))
        
        # Fallback pattern
        match = re.search(r'(\d+) failures', output)
        if match:
            return int(match.group(1))
        
        return 0
    
    def _save_report_from_output(self, output: str, script: str, 
                                report_directory: str) -> None:
        """
        Save markdown report from analysis output.
        
        Args:
            output: Analysis output containing markdown
            script: Script filename
            report_directory: Directory to save report
        """
        try:
            os.makedirs(report_directory, exist_ok=True)
            
            # Extract script name without extension for report filename
            script_name = Path(script).stem
            report_filename = f"{report_directory}/{script_name}_report.md"
            
            # Find the markdown content (everything after the summary header)
            lines = output.split('\n')
            markdown_start = -1
            
            for i, line in enumerate(lines):
                if line.strip().startswith('#') and 'Summary' in line:
                    markdown_start = i
                    break
            
            if markdown_start >= 0:
                markdown_content = '\n'.join(lines[markdown_start:])
                
                with open(report_filename, 'w', encoding='utf-8') as f:
                    f.write(markdown_content)
                    
                print(f"📄 Report saved: {report_filename}")
            else:
                print(f"⚠️  Could not extract markdown from {script} output")
                
        except Exception as e:
            print(f"⚠️  Report generation failed for {script}: {str(e)}")
    
    def run_all_analyses(self, *, test_mode: bool = False, generate_reports: bool = False, 
                        report_directory: str = "reports", max_workers: int = 1) -> None:
        """
        Run all analysis loopers in parallel.
        
        Args:
            test_mode: If True, run in test mode (4 files from 10 random subdirs)
            generate_reports: If True, generate markdown report files
            report_directory: Directory to save reports (if generate_reports=True)
            max_workers: Maximum number of parallel workers (default: number of CPUs)
        """
        print("=" * 80)
        print("JSON DATA MINE - COMPREHENSIVE FHIR ANALYSIS")
        print("=" * 80)
        print(f"Execution Mode: PARALLEL")
        print(f"Analysis Mode: {'TEST MODE' if test_mode else 'FULL ANALYSIS'}")
        print(f"Total Analyses: {len(self.looper_configs)}")
        print(f"Max Workers: {max_workers or os.cpu_count()}")
        print(f"Report Generation: {'ENABLED' if generate_reports else 'DISABLED'}")
        if generate_reports:
            print(f"Report Directory: {report_directory}")
        print("=" * 80)
        
        # Track overall statistics
        total_start_time = time.time()
        
        print(f"\n🚀 Starting parallel analysis execution with {max_workers or os.cpu_count()} workers...")
        
        # Run all analyses in parallel
        analysis_results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all jobs and show startup progress
            print("\n📋 Submitting analysis jobs:")
            future_to_config = {}
            
            for i, config in enumerate(self.looper_configs, 1):
                print(f"🔄 [{i:2}/{len(self.looper_configs)}] Starting: {config['name']}")
                
                future = executor.submit(
                    self._run_single_analysis,
                    config=config,
                    test_mode=test_mode,
                    generate_reports=generate_reports,
                    report_directory=report_directory
                )
                future_to_config[future] = config
                
                # Small delay to make startup visible
                time.sleep(0.1)
            
            print(f"\n✨ All {len(self.looper_configs)} analyses submitted and running in parallel!")
            print("⏳ Waiting for completion...\n")
            
            # Collect results as they complete
            completed_count = 0
            active_count = len(self.looper_configs)
            
            for future in concurrent.futures.as_completed(future_to_config):
                config = future_to_config[future]
                completed_count += 1
                active_count -= 1
                
                try:
                    result = future.result()
                    analysis_results.append(result)
                    
                    status = "✅" if result['success'] else "❌"
                    progress_bar = "█" * completed_count + "░" * active_count
                    
                    print(f"{status} [{completed_count:2}/{len(self.looper_configs)}] "
                          f"{result['name']} - {result['time']:.2f}s")
                    print(f"    Progress: [{progress_bar}] {completed_count}/{len(self.looper_configs)} complete, "
                          f"{active_count} still running")
                    
                    if result['success']:
                        files_msg = f"({result['processed']:,} files" 
                        if result['failures'] > 0:
                            files_msg += f", {result['failures']} failures"
                        files_msg += ")"
                        print(f"    Results: {files_msg}")
                    else:
                        print(f"    Error: {result.get('error', 'Unknown error')}")
                        
                    print()  # Add blank line for readability
                        
                except Exception as e:
                    analysis_results.append({
                        'name': config['name'],
                        'script': config['script'],
                        'processed': 0,
                        'failures': 0,
                        'time': 0,
                        'success': False,
                        'error': f'Future exception: {str(e)}'
                    })
                    active_count -= 1
                    progress_bar = "█" * completed_count + "░" * active_count
                    
                    print(f"❌ [{completed_count:2}/{len(self.looper_configs)}] "
                          f"{config['name']} - Future failed: {str(e)}")
                    print(f"    Progress: [{progress_bar}] {completed_count}/{len(self.looper_configs)} complete, "
                          f"{active_count} still running\n")
        
        # Print final summary
        total_time = time.time() - total_start_time
        self._print_final_summary(analysis_results, total_time, test_mode, generate_reports)
    
    def _print_final_summary(self, results: List[Dict], total_time: float, 
                           test_mode: bool, generate_reports: bool) -> None:
        """
        Print a comprehensive final summary of all analyses.
        
        Args:
            results: List of analysis result dictionaries
            total_time: Total execution time
            test_mode: Whether analyses were run in test mode
            generate_reports: Whether reports were generated
        """
        print("\n" + "=" * 80)
        print("PARALLEL ANALYSIS COMPLETE")
        print("=" * 80)
        
        # Overall statistics
        successful = sum(1 for r in results if r['success'])
        failed = len(results) - successful
        total_processed = sum(r['processed'] for r in results if r['success'])
        total_failures = sum(r['failures'] for r in results if r['success'])
        
        print(f"Overall Statistics:")
        print(f"  • Total Runtime: {total_time:.2f} seconds")
        print(f"  • Execution Mode: PARALLEL")
        print(f"  • Analyses Run: {len(results)}")
        print(f"  • Successful: {successful}")
        print(f"  • Failed: {failed}")
        print(f"  • Files Processed: {total_processed:,}")
        print(f"  • Processing Failures: {total_failures}")
        print(f"  • Mode: {'Test Mode' if test_mode else 'Full Analysis'}")
        
        if generate_reports:
            successful_reports = sum(1 for r in results if r['success'])
            print(f"  • Reports Generated: {successful_reports}")
        
        # Calculate speedup
        if successful > 0:
            avg_analysis_time = sum(r['time'] for r in results if r['success']) / successful
            theoretical_sequential_time = avg_analysis_time * successful
            speedup = theoretical_sequential_time / total_time if total_time > 0 else 1
            print(f"  • Parallel Speedup: {speedup:.1f}x")
        
        print("\nDetailed Results:")
        print("-" * 80)
        
        # Sort results by success status and then by name
        sorted_results = sorted(results, key=lambda x: (not x['success'], x['name']))
        
        for result in sorted_results:
            status = "✅ SUCCESS" if result['success'] else "❌ FAILED"
            script_name = Path(result['script']).stem
            
            if result['success']:
                print(f"{status:12} | {script_name:25} | "
                      f"Files: {result['processed']:6,} | "
                      f"Fails: {result['failures']:4} | "
                      f"Time: {result['time']:6.2f}s")
            else:
                error_msg = result.get('error', 'Unknown error')[:30]
                print(f"{status:12} | {script_name:25} | "
                      f"Error: {error_msg}")
        
        print("=" * 80)
        
        if failed > 0:
            print(f"⚠️  {failed} analyses failed. Check error messages above.")
        else:
            print("🎉 All analyses completed successfully!")
        
        print("=" * 80)


def main():
    """Main entry point for the analysis runner."""
    parser = argparse.ArgumentParser(
        description="Run comprehensive FHIR JSON data mining analysis in parallel",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python go.py                           # Run full analysis with reports (parallel)
  python go.py --test-mode              # Run test analysis with reports (parallel)
  python go.py --no-reports             # Run without generating reports
  python go.py --test-mode --no-reports # Test mode without reports  
  python go.py --workers 4              # Use 4 parallel workers
  python go.py --report-dir my_reports  # Custom report directory
        """
    )
    
    parser.add_argument(
        '--test-mode', 
        action='store_true',
        help='Run in test mode (4 files from 10 random subdirectories)'
    )
    
    parser.add_argument(
        '--no-reports',
        action='store_true',
        help='Skip generating markdown reports (reports are generated by default)'
    )
    
    parser.add_argument(
        '--report-dir',
        type=str,
        default='reports',
        help='Directory to save reports (default: reports)'
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        default=None,
        help='Maximum number of parallel workers (default: number of CPUs)'
    )
    
    parser.add_argument(
        '--list-analyses',
        action='store_true',
        help='List all available analyses and exit'
    )
    
    args = parser.parse_args()
    
    runner = AnalysisRunner()
    
    # Handle list option
    if args.list_analyses:
        print("Available Analyses:")
        print("=" * 60)
        for i, config in enumerate(runner.looper_configs, 1):
            script_name = Path(config['script']).stem
            print(f"{i:2}. {config['name']}")
            print(f"    Script: {config['script']}")
            print(f"    Class: {script_name}")
            print(f"    Description: {config['description']}")
            print()
        return
    
    try:
        runner.run_all_analyses(
            test_mode=args.test_mode,
            generate_reports=not args.no_reports,
            report_directory=args.report_dir,
            max_workers=args.workers
        )
    except KeyboardInterrupt:
        print("\n\nAnalysis interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\nFatal error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
