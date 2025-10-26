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
    
    
    def _get_example_files(self, *, category: str) -> Tuple[str, str, str]:
        """
        Get three example files for a category: longest, shortest, and random.
        
        Args:
            category: The category to get examples for
            
        Returns:
            Tuple of (longest_web_url, shortest_web_url, random_web_url)
        """
        # Use the parent's generic method
        # Tuple structure: (filename, filename_length, relative_path)
        # metric_index=1 (filename_length), relative_path_index=2
        return self.get_example_files_by_metric(
            examples=self.meta_examples[category], 
            metric_index=1, 
            relative_path_index=2
        )
    
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
