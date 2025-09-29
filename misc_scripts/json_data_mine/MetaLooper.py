#!/usr/bin/env python3

"""
Meta Tag Analysis Looper

Analyzes the "meta" tag in FHIR JSON files.
- Tracks presence of meta tag and its subfields: versionId, lastUpdated, source
- Identifies unknown keys beyond the expected ones
- Provides longest/shortest/random examples based on filename length
"""

import json
import random
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Set
from collections import defaultdict
from EndPointLooperParent import EndPointLooperParent, run_endpoint_analyzer


class MetaLooper(EndPointLooperParent):
    """
    Child class that analyzes meta tag structure in FHIR JSON files.
    """
    
    def __init__(self):
        """Initialize the meta tag analyzer with data tracking structures."""
        super().__init__()
        
        # Track meta analysis results
        self.meta_counts: Dict[str, int] = defaultdict(int)
        
        # Track example files with filename lengths and relative paths
        # Structure: {category: [(filename, filename_length, relative_path), ...]}
        self.meta_examples: Dict[str, List[Tuple[str, int, str]]] = defaultdict(list)
        
        # Track unknown meta keys
        self.unknown_meta_keys: Set[str] = set()
        
        # Track current file's relative path
        self.current_relative_path = ""
        
        # Expected meta keys
        self.expected_keys = {'versionId', 'lastUpdated', 'source'}
    
    def analyze_this_json_data(self, *, json_data: dict, source_filename: str) -> None:
        """
        Analyze JSON data to extract meta tag information.
        
        Args:
            json_data: The JSON data as a dictionary
            source_filename: Name of the source file being processed
        """
        # Extract relative path - will be set properly in run_loop
        if not self.current_relative_path:
            self.current_relative_path = f"unknown_dir/{source_filename}"
        
        # Look for meta field under the 'resource' element
        has_meta = False
        has_version_id = False
        has_last_updated = False
        has_source = False
        
        if 'resource' in json_data and isinstance(json_data['resource'], dict):
            resource = json_data['resource']
            meta_field = resource.get('meta')
            
            if isinstance(meta_field, dict) and meta_field:
                has_meta = True
                
                # Check for expected subfields
                has_version_id = 'versionId' in meta_field
                has_last_updated = 'lastUpdated' in meta_field
                has_source = 'source' in meta_field
                
                # Check for unknown keys
                for key in meta_field.keys():
                    if key not in self.expected_keys:
                        self.unknown_meta_keys.add(key)
        
        # Categorize meta presence
        if has_meta:
            self.meta_counts['has_meta'] += 1
            
            if has_version_id:
                self.meta_counts['has_version_id'] += 1
            if has_last_updated:
                self.meta_counts['has_last_updated'] += 1
            if has_source:
                self.meta_counts['has_source'] += 1
        else:
            self.meta_counts['no_meta'] += 1
        
        # Store examples for relevant categories
        categories_to_store = []
        if has_meta:
            categories_to_store.append('has_meta')
            if has_version_id:
                categories_to_store.append('has_version_id')
            if has_last_updated:
                categories_to_store.append('has_last_updated')
            if has_source:
                categories_to_store.append('has_source')
        else:
            categories_to_store.append('no_meta')
        
        for category in categories_to_store:
            if len(self.meta_examples[category]) < 10:
                self.meta_examples[category].append((
                    source_filename,
                    len(source_filename),
                    self.current_relative_path
                ))
    
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
        examples_with_lengths = self.meta_examples[category]
        
        if not examples_with_lengths:
            return "None", "None", "None"
        
        # Sort by filename length to find longest and shortest
        sorted_examples = sorted(examples_with_lengths, key=lambda x: x[1])
        
        # Get longest (last in sorted list)
        longest_entry = sorted_examples[-1]
        longest_url = self.get_web_url_of_cache_file(relative_path=longest_entry[2])
        
        # Get shortest (first in sorted list)
        shortest_entry = sorted_examples[0]
        shortest_url = self.get_web_url_of_cache_file(relative_path=shortest_entry[2])
        
        # Get random example
        random_entry = random.choice(examples_with_lengths)
        random_url = self.get_web_url_of_cache_file(relative_path=random_entry[2])
        
        return longest_url, shortest_url, random_url
    
    def _get_descriptive_category_name(self, *, category: str) -> str:
        """
        Get descriptive name for meta categories.
        
        Args:
            category: The internal category name
            
        Returns:
            Descriptive category name for display
        """
        category_descriptions = {
            'has_meta': 'Has Meta Tag',
            'no_meta': 'No Meta Tag',
            'has_version_id': 'Has versionId',
            'has_last_updated': 'Has lastUpdated',
            'has_source': 'Has source'
        }
        
        return category_descriptions.get(category, category.replace('_', ' ').title())
    
    def generate_summary_markdown(self) -> str:
        """
        Generate a comprehensive summary of meta tag analysis as markdown string.
        
        Returns:
            String containing the markdown report
        """
        lines = []
        lines.append("# Meta Tag Analysis Summary")
        lines.append("")
        lines.append("## What This Analysis Does")
        lines.append("This analysis examines the \"meta\" tag structure in FHIR JSON files. It tracks the ")
        lines.append("presence of meta tags and their expected subfields (versionId, lastUpdated, source) ")
        lines.append("while also identifying any unknown keys that appear beyond the expected ones.")
        lines.append("")
        lines.append("- **Expected Fields:** versionId, lastUpdated, source")
        lines.append("- **Categories:** Has meta, no meta, individual subfield presence")
        lines.append("- **Special Feature:** Reports unknown meta keys beyond expected ones")
        lines.append("- **Examples:** Longest/shortest/random by filename length")
        lines.append("")
        lines.append("## Processing Results")
        lines.append(f"**Files Processed:** {self.processed_count}")
        lines.append(f"**Files Failed:** {self.failure_count}")
        lines.append(f"**Total Meta Categories:** {len(self.meta_counts)}")
        lines.append("")
        
        if not self.meta_counts:
            lines.append("No meta tag data found in processed files.")
        else:
            lines.append("## Meta Tag Distribution")
            lines.append("")
            lines.append("| Meta Category | Count | Longest Example | Shortest Example | Random Example |")
            lines.append("|---------------|-------|-----------------|------------------|----------------|")
            
            # Sort by count (descending) then by category name
            sorted_categories = sorted(
                self.meta_counts.items(),
                key=lambda x: (-x[1], x[0])
            )
            
            for category, count in sorted_categories:
                longest, shortest, random_example = self._get_example_files(category=category)
                
                # Create readable category names
                category_display = self._get_descriptive_category_name(category=category)
                
                lines.append(f"| {category_display} | {count} | {longest} | {shortest} | {random_example} |")
            
            lines.append("")
            lines.append(f"**Total Files Analyzed:** {self.processed_count}")
            
            # Add unknown meta keys section
            if self.unknown_meta_keys:
                lines.append("")
                lines.append("## Unknown Meta Keys Found")
                lines.append("")
                lines.append("The following meta keys were found beyond versionId, lastUpdated, and source:")
                lines.append("")
                for key in sorted(self.unknown_meta_keys):
                    lines.append(f"* `{key}`")
        
        return "\n".join(lines)

    def print_summary(self) -> None:
        """
        Print a comprehensive summary of meta tag analysis.
        """
        print("\n" + self.generate_summary_markdown())


if __name__ == "__main__":
    run_endpoint_analyzer(
        analyzer_class=MetaLooper,
        description="Analyze meta tag structure in FHIR JSON files"
    )
