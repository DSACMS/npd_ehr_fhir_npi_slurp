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
    
    def run_loop(self, *, test_mode: bool = False) -> None:
        """Standard run_loop implementation."""
        print("Starting JSON processing...")
        print(f"Test mode: {'Enabled' if test_mode else 'Disabled'}")
        
        try:
            cache_directory = self.load_environment_config()
            print(f"Cache directory: {cache_directory}")
            
            json_files = self.discover_json_files(cache_directory=cache_directory, test_mode=test_mode)
            print(f"Found {self.total_files_found} JSON files to process")
            
            if not json_files:
                print("EndPointLooperParent Warning: No JSON files found to process")
                self.print_summary()
                return
            
            print("Processing JSON files...")
            for json_file_path in json_files:
                try:
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
                    
                    self.analyze_this_json_data(json_data=json_data, source_filename=str(json_file_path.name))
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
        """Get three example files for a category: longest, shortest, and random."""
        examples_with_lengths = self.payload_examples[category]
        
        if not examples_with_lengths:
            return "None", "None", "None"
        
        sorted_examples = sorted(examples_with_lengths, key=lambda x: x[1])
        
        longest_entry = sorted_examples[-1]
        longest_url = self.get_web_url_of_cache_file(relative_path=longest_entry[2])
        
        shortest_entry = sorted_examples[0]
        shortest_url = self.get_web_url_of_cache_file(relative_path=shortest_entry[2])
        
        random_entry = random.choice(examples_with_lengths)
        random_url = self.get_web_url_of_cache_file(relative_path=random_entry[2])
        
        return longest_url, shortest_url, random_url
    
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
