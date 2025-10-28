"""
CSV export utilities with PostgreSQL compatibility
"""
import csv
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime


class CSVExporter:
    """Enhanced CSV export with PostgreSQL compatibility"""
    
    @staticmethod
    def export_dataframe_to_csv(*, df: pd.DataFrame, file_path: Path, table_name: str = "") -> bool:
        """Export pandas DataFrame to PostgreSQL-compatible CSV"""
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            df.to_csv(
                file_path,
                index=False,
                na_rep='',  # Empty string for NULL values
                quoting=csv.QUOTE_MINIMAL,
                date_format='%Y-%m-%d %H:%M:%S',
                encoding='utf-8'
            )
            
            print(f"Exported {len(df)} records to {file_path}")
            return True
            
        except Exception as e:
            print(f"Error exporting {table_name} to {file_path}: {e}")
            return False
    
    @staticmethod
    def export_records_to_csv(*, records: List[Dict[str, Any]], file_path: Path, fieldnames: Optional[List[str]] = None) -> bool:
        """Export list of records to PostgreSQL-compatible CSV"""
        if not records:
            print(f"No records to export to {file_path}")
            return False
        
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Determine fieldnames if not provided
            if fieldnames is None:
                fieldnames = list(records[0].keys())
            
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
                writer.writeheader()
                
                for record in records:
                    # Convert None values to empty strings
                    cleaned_record = {}
                    for key, value in record.items():
                        if value is None:
                            cleaned_record[key] = ''
                        elif isinstance(value, datetime):
                            cleaned_record[key] = value.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            cleaned_record[key] = value
                    
                    writer.writerow(cleaned_record)
            
            print(f"Exported {len(records)} records to {file_path}")
            return True
            
        except Exception as e:
            print(f"Error exporting records to {file_path}: {e}")
            return False
    
    @staticmethod
    def generate_postgresql_copy_commands(*, csv_directory: Path, table_prefix: str = "") -> List[str]:
        """Generate PostgreSQL COPY commands for CSV import"""
        commands = []
        
        for csv_file in csv_directory.glob("*.csv"):
            table_name = csv_file.stem
            if table_prefix:
                table_name = f"{table_prefix}_{table_name}"
            
            # Generate COPY command
            copy_command = f"COPY {table_name} FROM '{csv_file.absolute()}' WITH CSV HEADER;"
            commands.append(copy_command)
        
        return commands
    
    @staticmethod
    def save_copy_commands_to_file(*, commands: List[str], output_file: Path):
        """Save PostgreSQL COPY commands to a SQL file"""
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write("-- PostgreSQL COPY commands for FHIR data import\n")
                f.write(f"-- Generated on {datetime.now().isoformat()}\n\n")
                
                for command in commands:
                    f.write(f"{command}\n")
            
            print(f"Saved {len(commands)} COPY commands to {output_file}")
            
        except Exception as e:
            print(f"Error saving COPY commands to {output_file}: {e}")
