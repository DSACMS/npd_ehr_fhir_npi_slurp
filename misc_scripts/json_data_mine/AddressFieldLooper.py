#!/usr/bin/env python3

"""
Address Field Classification Looper

Analyzes address field contents using regex classification from parent class.
- Uses classify_address_content static function to categorize address values
- Tracks address field presence and classification results
- Provides longest/shortest/random examples based on filename length
"""

import json
import random
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Set
from collections import defaultdict
from EndPointLooperParent import EndPointLooperParent, run_endpoint_analyzer


class AddressFieldLooper(EndPointLooperParent):
    """
    Child class that classifies address field contents in FHIR JSON files.
    """
    
    def __init__(self):
        """Initialize the address field classifier with data tracking structures."""
        super().__init__()
        
        # Track address classification results
        self.address_classification_counts: Dict[str, int] = defaultdict(int)
        
        # Track example files with filename lengths and relative paths
        # Structure: {category: [(address_value, filename_length, filename, relative_path), ...]}
        self.address_examples: Dict[str, List[Tuple[str, int, str, str]]] = defaultdict(list)
        
        # Track files without address fields
        self.files_without_address = 0
        
        # Track current file's relative path
        self.current_relative_path = ""
    
    def analyze_this_json_data(self, *, json_data: dict, source_filename: str) -> None:
        """
        Analyze JSON data to classify address field contents.
        
        Args:
            json_data: The JSON data as a dictionary
            source_filename: Name of the source file being processed
        """
        # Extract relative path - will be set properly in run_loop
        if not self.current_relative_path:
            self.current_relative_path = f"unknown_dir/{source_filename}"
        
        # Look for address field under the 'resource' element
        addresses_found = []
        
        if 'resource' in json_data and isinstance(json_data['resource'], dict):
            resource = json_data['resource']
            address_field = resource.get('address')
            
            if address_field is not None:
                # Address can be a list or single dict
                if isinstance(address_field, list):
                    for addr in address_field:
                        if isinstance(addr, dict):
                            # Extract address components and classify them
                            for key, value in addr.items():
                                if isinstance(value, str) and value.strip():
                                    addresses_found.append(value.strip())
                                elif isinstance(value, list):
                                    for item in value:
                                        if isinstance(item, str) and item.strip():
                                            addresses_found.append(item.strip())
                elif isinstance(address_field, dict):
                    # Extract address components and classify them
                    for key, value in address_field.items():
                        if isinstance(value, str) and value.strip():
                            addresses_found.append(value.strip())
                        elif isinstance(value, list):
                            for item in value:
                                if isinstance(item, str) and item.strip():
                                    addresses_found.append(item.strip())
        
        if addresses_found:
            for address_value in addresses_found:
                # Classify using parent class method
                classification = self.classify_address_content(content=address_value)
                
                if classification:
                    category = classification
                else:
                    category = 'unclassified'
                
                # Count this category
                self.address_classification_counts[category] += 1
                
                # Store example (limit to prevent memory issues)
                if len(self.address_examples[category]) < 10:
                    self.address_examples[category].append((
                        address_value,
                        len(source_filename),
                        source_filename,
                        self.current_relative_path
                    ))
        else:
            # Track files without address fields
            self.files_without_address += 1
    
    
    def _get_example_files(self, *, category: str) -> Tuple[str, str, str]:
        """
        Get three example files for a category: longest, shortest, and random.
        """
        # Use the parent's generic method
        # Tuple structure: (address_value, filename_length, filename, relative_path)
        # metric_index=1 (filename_length), relative_path_index=3
        return self.get_example_files_by_metric(
            examples=self.address_examples[category], 
            metric_index=1, 
            relative_path_index=3
        )
    
    def generate_summary_markdown(self) -> str:
        """
        Generate a comprehensive summary of address classification as markdown string.
        """
        lines = []
        lines.append("# Address Field Classification Summary")
        lines.append(f"**Files Processed:** {self.processed_count}")
        lines.append(f"**Files Failed:** {self.failure_count}")
        lines.append(f"**Files Without Address Fields:** {self.files_without_address}")
        lines.append(f"**Total Classification Categories:** {len(self.address_classification_counts)}")
        lines.append("")
        
        if not self.address_classification_counts:
            lines.append("No address field content found in processed files.")
        else:
            lines.append("## Address Classification Results")
            lines.append("")
            lines.append("| Classification | Count | Longest Example | Shortest Example | Random Example |")
            lines.append("|----------------|-------|-----------------|------------------|----------------|")
            
            # Sort by count (descending) then by category name
            sorted_categories = sorted(
                self.address_classification_counts.items(),
                key=lambda x: (-x[1], x[0])
            )
            
            for category, count in sorted_categories:
                longest, shortest, random_example = self._get_example_files(category=category)
                category_display = category.replace('_', ' ').title()
                lines.append(f"| {category_display} | {count} | {longest} | {shortest} | {random_example} |")
            
            lines.append("")
            lines.append(f"**Total Address Components Classified:** {sum(self.address_classification_counts.values())}")
        
        return "\n".join(lines)

    def print_summary(self) -> None:
        """
        Print a comprehensive summary of address classification.
        """
        print("\n" + self.generate_summary_markdown())


if __name__ == "__main__":
    run_endpoint_analyzer(
        analyzer_class=AddressFieldLooper,
        description="Classify address field contents in FHIR JSON files"
    )
