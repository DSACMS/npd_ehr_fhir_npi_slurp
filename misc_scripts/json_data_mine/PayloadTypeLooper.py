#!/usr/bin/env python3

"""
Payload Type Analysis Looper

Analyzes payloadType fields in FHIR JSON files including coding subkeys and address classification.
- Analyzes system/code values under coding subkey
- Classifies address subkey contents using parent class method
- Tracks header subkey presence
- Provides longest/shortest/random examples based on filename length
"""

import json
import random
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Set
from collections import defaultdict
from EndPointLooperParent import EndPointLooperParent, run_endpoint_analyzer


class PayloadTypeLooper(EndPointLooperParent):
    """
    Child class that analyzes payloadType fields in FHIR JSON files.
    """
    
    def __init__(self):
        """Initialize the payload type analyzer with data tracking structures."""
        super().__init__()
        
        # Track payload type analysis results
        self.payload_counts: Dict[str, int] = defaultdict(int)
        self.system_counts: Dict[str, int] = defaultdict(int)
        self.code_counts: Dict[str, int] = defaultdict(int)
        self.address_classification_counts: Dict[str, int] = defaultdict(int)
        
        # Track example files with filename lengths and relative paths
        self.payload_examples: Dict[str, List[Tuple[str, int, str]]] = defaultdict(list)
        
        # Track current file's relative path
        self.current_relative_path = ""
    
    def analyze_this_json_data(self, *, json_data: dict, source_filename: str) -> None:
        """
        Analyze JSON data to extract payloadType information.
        
        Args:
            json_data: The JSON data as a dictionary
            source_filename: Name of the source file being processed
        """
        # Extract relative path - will be set properly in run_loop
        if not self.current_relative_path:
            self.current_relative_path = f"unknown_dir/{source_filename}"
        
        # Look for payloadType field under the 'resource' element
        has_payload_type = False
        has_coding = False
        has_address = False
        has_header = False
        
        if 'resource' in json_data and isinstance(json_data['resource'], dict):
            resource = json_data['resource']
            payload_type = resource.get('payloadType')
            
            if payload_type is not None:
                has_payload_type = True
                
                # payloadType can be list or single dict
                payload_types_to_analyze = []
                if isinstance(payload_type, list):
                    payload_types_to_analyze.extend([pt for pt in payload_type if isinstance(pt, dict)])
                elif isinstance(payload_type, dict):
                    payload_types_to_analyze.append(payload_type)
                
                for pt in payload_types_to_analyze:
                    # Analyze coding subkey
                    coding = pt.get('coding')
                    if coding is not None:
                        has_coding = True
                        if isinstance(coding, list):
                            for code_entry in coding:
                                if isinstance(code_entry, dict):
                                    system = code_entry.get('system')
                                    code = code_entry.get('code')
                                    if system:
                                        self.system_counts[str(system)] += 1
                                    if code:
                                        self.code_counts[str(code)] += 1
                        elif isinstance(coding, dict):
                            system = coding.get('system')
                            code = coding.get('code')
                            if system:
                                self.system_counts[str(system)] += 1
                            if code:
                                self.code_counts[str(code)] += 1
                    
                    # Analyze address subkey
                    address = pt.get('address')
                    if address is not None and isinstance(address, str):
                        has_address = True
                        # Classify using parent class method
                        classification = self.classify_address_content(content=address)
                        if classification:
                            self.address_classification_counts[classification] += 1
                        else:
                            self.address_classification_counts['unclassified'] += 1
                    
                    # Check for header subkey
                    header = pt.get('header')
                    if header is not None:
                        has_header = True
        
        # Count categories
        if has_payload_type:
            self.payload_counts['has_payload_type'] += 1
        else:
            self.payload_counts['no_payload_type'] += 1
        
        if has_coding:
            self.payload_counts['has_coding'] += 1
        if has_address:
            self.payload_counts['has_address'] += 1
        if has_header:
            self.payload_counts['has_header'] += 1
        
        # Store examples
        category = 'has_payload_type' if has_payload_type else 'no_payload_type'
        if len(self.payload_examples[category]) < 10:
            self.payload_examples[category].append((
                source_filename,
                len(source_filename),
                self.current_relative_path
            ))
    
    
    def _get_example_files(self, *, category: str) -> Tuple[str, str, str]:
        """Get three example files for a category: longest, shortest, and random."""
        # Use the parent's generic method
        # Tuple structure: (filename, filename_length, relative_path)
        # metric_index=1 (filename_length), relative_path_index=2
        return self.get_example_files_by_metric(
            examples=self.payload_examples[category], 
            metric_index=1, 
            relative_path_index=2
        )
    
    def generate_summary_markdown(self) -> str:
        """Generate a comprehensive summary of payloadType analysis as markdown string."""
        lines = []
        lines.append("# Payload Type Analysis Summary")
        lines.append("")
        lines.append("## What This Analysis Does")
        lines.append("This analysis examines payloadType fields in FHIR JSON files, including their coding ")
        lines.append("subkeys and address classification. It analyzes system/code values under coding subkeys, ")
        lines.append("classifies address subkey contents using regex patterns, and tracks header subkey presence.")
        lines.append("")
        lines.append("- **Coding Analysis:** System and code statistics from coding subkey")
        lines.append("- **Address Classification:** Uses regex classification on address subkey contents")
        lines.append("- **Header Detection:** Tracks presence of header subkey")
        lines.append("- **Categories:** Has payloadType, coding, address, header presence")
        lines.append("- **Examples:** Longest/shortest/random by filename length")
        lines.append("")
        lines.append("## Processing Results")
        lines.append(f"**Files Processed:** {self.processed_count}")
        lines.append(f"**Files Failed:** {self.failure_count}")
        lines.append("")
        
        # PayloadType presence
        if self.payload_counts:
            lines.append("## Payload Type Presence")
            lines.append("")
            lines.append("| Category | Count | Longest Example | Shortest Example | Random Example |")
            lines.append("|----------|-------|-----------------|------------------|----------------|")
            
            for category in ['has_payload_type', 'no_payload_type', 'has_coding', 'has_address', 'has_header']:
                if category in self.payload_counts:
                    count = self.payload_counts[category]
                    if category in ['has_payload_type', 'no_payload_type']:
                        longest, shortest, random_example = self._get_example_files(category=category)
                    else:
                        longest = shortest = random_example = "N/A"
                    category_display = category.replace('_', ' ').title()
                    lines.append(f"| {category_display} | {count} | {longest} | {shortest} | {random_example} |")
        
        # System statistics
        if self.system_counts:
            lines.append("")
            lines.append("## Coding System Values")
            lines.append("")
            lines.append("| System | Count |")
            lines.append("|---------|-------|")
            for system, count in sorted(self.system_counts.items(), key=lambda x: (-x[1], x[0])):
                lines.append(f"| `{system}` | {count} |")
        
        # Code statistics  
        if self.code_counts:
            lines.append("")
            lines.append("## Coding Code Values")
            lines.append("")
            lines.append("| Code | Count |")
            lines.append("|------|-------|")
            for code, count in sorted(self.code_counts.items(), key=lambda x: (-x[1], x[0])):
                lines.append(f"| `{code}` | {count} |")
        
        # Address classification
        if self.address_classification_counts:
            lines.append("")
            lines.append("## Address Classification")
            lines.append("")
            lines.append("| Classification | Count |")
            lines.append("|----------------|-------|")
            for classification, count in sorted(self.address_classification_counts.items(), key=lambda x: (-x[1], x[0])):
                classification_display = classification.replace('_', ' ').title()
                lines.append(f"| {classification_display} | {count} |")
        
        return "\n".join(lines)

    def print_summary(self) -> None:
        """Print a comprehensive summary of payloadType analysis."""
        print("\n" + self.generate_summary_markdown())


if __name__ == "__main__":
    run_endpoint_analyzer(
        analyzer_class=PayloadTypeLooper,
        description="Analyze payloadType fields in FHIR JSON files"
    )
