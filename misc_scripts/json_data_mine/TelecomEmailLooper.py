#!/usr/bin/env python3

"""
Telecom Email Analysis Looper

Analyzes telecom entries in FHIR JSON files that have "email" as the system.
- Validates email addresses using regex
- Categorizes as valid or invalid
- Tracks files with no email telecoms
- Provides longest/shortest/random examples based on email character length
"""

import json
import random
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Set
from collections import defaultdict
from EndPointLooperParent import EndPointLooperParent, run_endpoint_analyzer


class TelecomEmailLooper(EndPointLooperParent):
    """
    Child class that analyzes telecom email entries in FHIR JSON files.
    """
    
    def __init__(self):
        """Initialize the telecom email analyzer with data tracking structures."""
        super().__init__()
        
        # Track email validation results
        self.email_counts: Dict[str, int] = defaultdict(int)
        
        # Track example files with email lengths and relative paths
        # Structure: {category: [(email_value, email_length, filename, relative_path), ...]}
        self.email_examples: Dict[str, List[Tuple[str, int, str, str]]] = defaultdict(list)
        
        # Track files without email telecoms
        self.files_without_email = 0
        
        # Track current file's relative path
        self.current_relative_path = ""
    
    def analyze_this_json_data(self, *, json_data: dict, source_filename: str) -> None:
        """
        Analyze JSON data to extract and validate telecom email information.
        
        Args:
            json_data: The JSON data as a dictionary
            source_filename: Name of the source file being processed
        """
        # Extract relative path for GitHub URLs
        cache_directory = self.load_environment_config()
        cache_path = Path(cache_directory)
        if not cache_path.exists():
            alternative_paths = [
                Path("../../../npd_ehr_scrape_cache/cehrt_fhir_json/"),  
                Path("../../npd_ehr_scrape_cache/cehrt_fhir_json/"),    
                Path("../npd_ehr_scrape_cache/cehrt_fhir_json/"),       
                Path("npd_ehr_scrape_cache/cehrt_fhir_json/"),          
            ]
            for alt_path in alternative_paths:
                if alt_path.exists():
                    cache_path = alt_path
                    break
        
        # Extract relative path - need to reconstruct from directory structure
        # Since we only have source_filename, we'll set a placeholder
        # This will be set properly when run_loop is called
        if not self.current_relative_path:
            self.current_relative_path = f"unknown_dir/{source_filename}"
        
        # Look for telecom array under the 'resource' element
        emails_found = []
        
        if 'resource' in json_data and isinstance(json_data['resource'], dict):
            resource = json_data['resource']
            telecoms = resource.get('telecom', [])
            
            if isinstance(telecoms, list):
                for telecom in telecoms:
                    if isinstance(telecom, dict):
                        system = telecom.get('system')
                        value = telecom.get('value')
                        
                        if system == 'email' and value:
                            emails_found.append(str(value))
        
        if emails_found:
            for email in emails_found:
                # Validate email
                if self.validate_email(email=email):
                    category = 'valid_email'
                else:
                    category = 'invalid_email'
                
                # Count this category
                self.email_counts[category] += 1
                
                # Store example (limit to prevent memory issues)
                if len(self.email_examples[category]) < 10:
                    self.email_examples[category].append((
                        email,
                        len(email),
                        source_filename,
                        self.current_relative_path
                    ))
        else:
            # Track files without email telecoms
            self.files_without_email += 1
    
    def run_loop(self, *, test_mode: bool = False) -> None:
        """
        Override parent run_loop to track relative paths for web URLs.
        """
        print("Starting JSON processing...")
        print(f"Test mode: {'Enabled' if test_mode else 'Disabled'}")
        
        try:
            # Load configuration
            cache_directory = self.load_environment_config()
            print(f"Cache directory: {cache_directory}")
            
            # Discover JSON files
            print("Discovering JSON files...")
            json_files = self.discover_json_files(
                cache_directory=cache_directory,
                test_mode=test_mode
            )
            print(f"Found {self.total_files_found} JSON files to process")
            
            if not json_files:
                print("EndPointLooperParent Warning: No JSON files found to process")
                self.print_summary()
                return
            
            # Process files
            print("Processing JSON files...")
            for json_file_path in json_files:
                try:
                    # Extract relative path for web URL generation
                    cache_path = Path(cache_directory)
                    if not cache_path.exists():
                        alternative_paths = [
                            Path("../../../npd_ehr_scrape_cache/cehrt_fhir_json/"),  
                            Path("../../npd_ehr_scrape_cache/cehrt_fhir_json/"),    
                            Path("../npd_ehr_scrape_cache/cehrt_fhir_json/"),       
                            Path("npd_ehr_scrape_cache/cehrt_fhir_json/"),          
                        ]
                        for alt_path in alternative_paths:
                            if alt_path.exists():
                                cache_path = alt_path
                                break
                    
                    try:
                        relative_path = str(json_file_path.relative_to(cache_path))
                        self.current_relative_path = relative_path
                    except ValueError:
                        self.current_relative_path = f"{json_file_path.parent.name}/{json_file_path.name}"
                    
                    with open(json_file_path, 'r', encoding='utf-8') as file:
                        json_data = json.load(file)
                    
                    # Call the child's analysis method
                    self.analyze_this_json_data(
                        json_data=json_data,
                        source_filename=str(json_file_path.name)
                    )
                    
                    self.processed_count += 1
                    
                    if self.processed_count % 100 == 0:
                        print(f"Processed {self.processed_count} files...")
                        
                except (json.JSONDecodeError, UnicodeDecodeError, IOError):
                    self.failure_count += 1
                    continue
            
            print(f"\nProcessing complete: {self.processed_count} files processed, {self.failure_count} failures")
            self.print_summary()
            
        except Exception as e:
            print(f"EndPointLooperParent Error: Processing failed: {str(e)}")
            raise
    
    def _get_example_files(self, *, category: str) -> Tuple[str, str, str]:
        """
        Get three example files for a category: longest, shortest, and random.
        
        Args:
            category: The category to get examples for
            
        Returns:
            Tuple of (longest_web_url, shortest_web_url, random_web_url)
        """
        examples_with_lengths = self.email_examples[category]
        
        if not examples_with_lengths:
            return "None", "None", "None"
        
        # Sort by email length to find longest and shortest
        sorted_examples = sorted(examples_with_lengths, key=lambda x: x[1])
        
        # Get longest (last in sorted list)
        longest_entry = sorted_examples[-1]
        longest_url = self.get_web_url_of_cache_file(relative_path=longest_entry[3])
        
        # Get shortest (first in sorted list)
        shortest_entry = sorted_examples[0]
        shortest_url = self.get_web_url_of_cache_file(relative_path=shortest_entry[3])
        
        # Get random example
        random_entry = random.choice(examples_with_lengths)
        random_url = self.get_web_url_of_cache_file(relative_path=random_entry[3])
        
        return longest_url, shortest_url, random_url
    
    def generate_summary_markdown(self) -> str:
        """
        Generate a comprehensive summary of telecom email analysis as markdown string.
        
        Returns:
            String containing the markdown report
        """
        lines = []
        lines.append("# Telecom Email Analysis Summary")
        lines.append("")
        lines.append("## What This Analysis Does")
        lines.append("This analysis examines telecom entries in FHIR JSON files that have \"email\" as the system value. ")
        lines.append("It validates email addresses using standard RFC-compliant regex patterns and categorizes them ")
        lines.append("as valid or invalid. Files without any email telecoms are also tracked.")
        lines.append("")
        lines.append("- **Validation Method:** Standard RFC-compliant email regex")
        lines.append("- **Categories:** Valid email, invalid email, no email telecoms")
        lines.append("- **Examples:** Longest/shortest/random by email character length")
        lines.append("")
        lines.append("## Processing Results")
        lines.append(f"**Files Processed:** {self.processed_count}")
        lines.append(f"**Files Failed:** {self.failure_count}")
        lines.append(f"**Files Without Email Telecoms:** {self.files_without_email}")
        lines.append(f"**Total Email Categories Found:** {len(self.email_counts)}")
        lines.append("")
        
        if not self.email_counts:
            lines.append("No telecom email entries found in processed files.")
        else:
            lines.append("## Email Validation Results")
            lines.append("")
            lines.append("| Email Category | Count | Longest Example | Shortest Example | Random Example |")
            lines.append("|----------------|-------|-----------------|------------------|----------------|")
            
            # Sort by count (descending) then by category name
            sorted_categories = sorted(
                self.email_counts.items(),
                key=lambda x: (-x[1], x[0])
            )
            
            for category, count in sorted_categories:
                longest, shortest, random_example = self._get_example_files(category=category)
                
                # Create readable category names
                category_display = category.replace('_', ' ').title()
                
                lines.append(f"| {category_display} | {count} | {longest} | {shortest} | {random_example} |")
            
            lines.append("")
            lines.append(f"**Total Email Telecoms Found:** {sum(self.email_counts.values())}")
        
        return "\n".join(lines)

    def print_summary(self) -> None:
        """
        Print a comprehensive summary of telecom email analysis.
        """
        print("\n" + self.generate_summary_markdown())


if __name__ == "__main__":
    run_endpoint_analyzer(
        analyzer_class=TelecomEmailLooper,
        description="Analyze telecom email entries in FHIR JSON files"
    )
