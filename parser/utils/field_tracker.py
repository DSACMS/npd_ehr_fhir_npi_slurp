"""
Field coverage tracking for JSON data processing
"""
from dataclasses import dataclass, field
from typing import Dict, Set, Any, List
import json


@dataclass
class FieldTracker:
    """Tracks which JSON fields are processed vs ignored"""
    processed_fields: Set[str] = field(default_factory=set)
    ignored_fields: Set[str] = field(default_factory=set)
    all_seen_fields: Set[str] = field(default_factory=set)
    
    def track_field_access(self, field_path: str, was_processed: bool = True):
        """Track whether a field was processed or ignored"""
        self.all_seen_fields.add(field_path)
        if was_processed:
            self.processed_fields.add(field_path)
        else:
            self.ignored_fields.add(field_path)
    
    def get_coverage_report(self) -> Dict[str, Any]:
        """Generate field coverage report"""
        total_fields = len(self.all_seen_fields)
        processed_count = len(self.processed_fields)
        ignored_count = len(self.ignored_fields)
        
        return {
            'total_fields_seen': total_fields,
            'fields_processed': processed_count,
            'fields_ignored': ignored_count,
            'coverage_percentage': (processed_count / total_fields * 100) if total_fields > 0 else 0,
            'ignored_fields_list': sorted(self.ignored_fields),
            'processed_fields_list': sorted(self.processed_fields)
        }


class JSONFieldWalker:
    """Walks through JSON structures and tracks field access"""
    
    def __init__(self, field_tracker: FieldTracker):
        self.field_tracker = field_tracker
    
    def walk_and_track(self, data: Dict[str, Any], prefix: str = "") -> None:
        """Walk JSON structure and track all field paths"""
        if not isinstance(data, dict):
            return
            
        for key, value in data.items():
            field_path = f"{prefix}.{key}" if prefix else key
            self.field_tracker.track_field_access(field_path, was_processed=False)  # Default to not processed
            
            # Recursively walk nested structures
            if isinstance(value, dict):
                self.walk_and_track(value, field_path)
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        self.walk_and_track(item, f"{field_path}[{i}]")
    
    def mark_field_processed(self, field_path: str):
        """Mark a specific field as processed"""
        self.field_tracker.track_field_access(field_path, was_processed=True)
    
    def extract_and_track_field(self, data: Dict[str, Any], field_name: str, default: Any = None, prefix: str = "") -> Any:
        """Extract a field value and mark it as processed"""
        field_path = f"{prefix}.{field_name}" if prefix else field_name
        
        if field_name in data:
            self.field_tracker.track_field_access(field_path, was_processed=True)
            return data[field_name]
        else:
            return default
