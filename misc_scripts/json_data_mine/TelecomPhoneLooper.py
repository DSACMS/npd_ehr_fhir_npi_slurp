#!/usr/bin/env python3

"""
Telecom Phone Analysis Looper

Analyzes telecom entries in FHIR JSON files that have "phone" as the system.
- Validates phone numbers (10-11 digits total)
- Categorizes as valid 10-digit, valid 11-digit, or invalid
- Tracks files with no phone telecoms
- Provides longest/shortest/random examples based on phone number character length
"""

import json
import random
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Set
from collections import defaultdict
from EndPointLooperParent import EndPointLooperParent, run_endpoint_analyzer


class TelecomPhoneLooper(EndPointLooperParent):
    """
    Child class that analyzes telecom phone entries in FHIR JSON files.
    """
    
    def __init__(self):
        """Initialize the telecom phone analyzer with data tracking structures."""
        super().__init__()
        
        # Track phone validation results
        self.phone_counts: Dict[str, int] = defaultdict(int)
        
        # Track example files with phone lengths and relative paths
        # Structure: {category: [(phone_value, phone_length, filename, relative_path), ...]}
        self.phone_examples: Dict[str, List[Tuple[str, int, str, str]]] = defaultdict(list)
        
        # Track files without phone telecoms
        self.files_without_phone = 0
        
        # Track current file's relative path
        self.current_relative_path = ""
    
    def analyze_this_json_data(self, *, json_data: dict, source_filename: str) -> None:
        """
        Analyze JSON data to extract and validate telecom phone information.
        
        Args:
            json_data: The JSON data as a dictionary
            source_filename: Name of the source file being processed
        """
        # Extract relative path - will be set properly in run_loop
        if not self.current_relative_path:
            self.current_relative_path = f"unknown_dir/{source_filename}"
        
        # Look for telecom array under the 'resource' element
        phones_found = []
        
        if 'resource' in json_data and isinstance(json_data['resource'], dict):
            resource = json_data['resource']
            telecoms = resource.get('telecom', [])
            
            if isinstance(telecoms, list):
                for telecom in telecoms:
                    if isinstance(telecom, dict):
                        system = telecom.get('system')
                        value = telecom.get('value')
                        
                        if system == 'phone' and value:
                            phones_found.append(str(value))
        
        if phones_found:
            for phone in phones_found:
                # Validate phone using parent class method
                validation_result = self.validate_phone(phone=phone)
                
                # Handle None case (shouldn't happen but for type safety)
                if validation_result is None:
                    validation_result = 'invalid'
                
                # Count this category
                self.phone_counts[validation_result] += 1
                
                # Store example (limit to prevent memory issues)
                if len(self.phone_examples[validation_result]) < 10:
                    self.phone_examples[validation_result].append((
                        phone,
                        len(phone),
                        source_filename,
                        self.current_relative_path
                    ))
        else:
            # Track files without phone telecoms
            self.files_without_phone += 1
    
    
    def _get_example_files(self, *, category: str) -> Tuple[str, str, str]:
        """
        Get three example files for a category: longest, shortest, and random.
        
        Args:
            category: The category to get examples for
            
        Returns:
            Tuple of (longest_web_url, shortest_web_url, random_web_url)
        """
        # Use the parent's generic method
        # Tuple structure: (phone_value, phone_length, filename, relative_path)
        # metric_index=1 (phone_length), relative_path_index=3
        return self.get_example_files_by_metric(
            examples=self.phone_examples[category], 
            metric_index=1, 
            relative_path_index=3
        )
    
    def _get_descriptive_category_name(self, *, category: str) -> str:
        """
        Get descriptive name for phone validation categories.
        
        Args:
            category: The internal category name
            
        Returns:
            Descriptive category name for display
        """
        category_descriptions = {
            'valid_10_digit': 'Valid 10-Digit Phone',
            'valid_11_digit': 'Valid 11-Digit Phone',
            'invalid': 'Invalid Phone Format'
        }
        
        return category_descriptions.get(category, category.replace('_', ' ').title())
    
    def generate_summary_markdown(self) -> str:
        """
        Generate a comprehensive summary of telecom phone analysis as markdown string.
        
        Returns:
            String containing the markdown report
        """
        lines = []
        lines.append("# Telecom Phone Analysis Summary")
        lines.append(f"**Files Processed:** {self.processed_count}")
        lines.append(f"**Files Failed:** {self.failure_count}")
        lines.append(f"**Files Without Phone Telecoms:** {self.files_without_phone}")
        lines.append(f"**Total Phone Categories Found:** {len(self.phone_counts)}")
        lines.append("")
        
        if not self.phone_counts:
            lines.append("No telecom phone entries found in processed files.")
        else:
            lines.append("## Phone Validation Results")
            lines.append("")
            lines.append("| Phone Category | Count | Longest Example | Shortest Example | Random Example |")
            lines.append("|----------------|-------|-----------------|------------------|----------------|")
            
            # Sort by count (descending) then by category name
            sorted_categories = sorted(
                self.phone_counts.items(),
                key=lambda x: (-x[1], x[0])
            )
            
            for category, count in sorted_categories:
                longest, shortest, random_example = self._get_example_files(category=category)
                
                # Create readable category names
                category_display = self._get_descriptive_category_name(category=category)
                
                lines.append(f"| {category_display} | {count} | {longest} | {shortest} | {random_example} |")
            
            lines.append("")
            lines.append(f"**Total Phone Telecoms Found:** {sum(self.phone_counts.values())}")
        
        return "\n".join(lines)

    def print_summary(self) -> None:
        """
        Print a comprehensive summary of telecom phone analysis.
        """
        print("\n" + self.generate_summary_markdown())


if __name__ == "__main__":
    run_endpoint_analyzer(
        analyzer_class=TelecomPhoneLooper,
        description="Analyze telecom phone entries in FHIR JSON files"
    )
