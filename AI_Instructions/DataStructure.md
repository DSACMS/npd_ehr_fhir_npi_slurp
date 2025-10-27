Data Structure
=======================

Under misc_scripts/json_data_mine is code and the reports that the code generates that reveals the structure of the Certified EHR Technology Vendor FHIR client urls. These are FHIR (JSON) files that describe the organizations and their endpoints. I would like to be able to represent these in a tabular manner. Step30_parse_source_bundle.py and Step40_extract_csv_data.py are my current attempts to do this. 

However, I do not think they accurately reflect how the FHIR data is structured.

Under ./data_model is a series of CSV files that represent a possible structure to model out the data from the JSON files. Please read all of these files. 

I would like you to compare the actual data (look through the markdown in reports undermisc_scripts/json_data_mine to find representative data structures) and read a few random file under ../npd_ehr_scrape_cache/cache/fhir_json_cache/ to get a feel for how the data is typically structured. 

The provide me with a comprehensive plan about how to properly store the JSON data in a tabular data format that can be easily used to find the needed details of organizational endpoints by searching for organizational NPIs. 

Please add your plan underneath this text, but do not delete this text:

## Comprehensive Plan

### New OOP Architecture for FHIR Data Processing

Based on analysis of the existing code and data structure, a new Object-Oriented approach has been designed to replace the current Step30/Step40 pipeline. This new architecture addresses several key requirements:

#### Key Design Principles

1. **Original ID Preservation**: Maintain both original JSON IDs and generate deterministic UUID5s for database stability
2. **Data Loss Tracking**: Log all JSON fields that are not captured during processing
3. **OOP & DRY**: Clean object-oriented design with shared functionality in base classes
4. **Direct PostgreSQL Mapping**: Generate CSV files that map directly to the fhir_tables.sql schema

#### Architecture Components

**1. Deterministic UUID5 Generation**
- Uses UUID5 with consistent namespaces for stable identifiers across processing runs
- Combines vendor_name::full_url::original_id as seed for reproducible UUIDs
- Separate namespaces for organizations, endpoints, and vendors

**2. Field Coverage Tracking**
- JSONFieldWalker class traverses all JSON structures
- FieldTracker maintains sets of processed vs ignored fields
- Generates coverage reports showing what data is "thrown on the floor"

**3. Enhanced FHIR Resource Classes**
- FHIRResource base class with original_id, uuid_id, and field_tracker
- FHIROrganization and FHIREndpoint subclasses for specific resource types
- Each resource tracks its data lineage and processing coverage

**4. PostgreSQL Table Management**
- PostgreSQLTableManager uses pandas DataFrames for each table
- Direct mapping to existing fhir_tables.sql schema
- Enhanced schema includes original_id columns and data_lineage tracking

**5. Processing Pipeline**
- VendorProcessor handles vendor directory structure (endpoint/ and organization/ subdirs)
- ResourceDirectoryProcessor with Factory pattern for different resource types
- FHIRCacheProcessor orchestrates the entire pipeline with comprehensive error handling

#### New Directory Structure

The cache structure has evolved to:
```
../npd_ehr_scrape_cache/cache/fhir_json_cache/
├── vendor_name_1/
│   ├── endpoint/
│   │   ├── entry_Endpoint_*.json
│   └── organization/
│       ├── entry_Organization_*.json
├── vendor_name_2/
│   ├── endpoint/
│   └── organization/
```

#### Implementation Location

The new implementation will be located in:
- `./parser/` - Main OOP classes and processing logic
- `./parser/models/` - FHIR resource classes
- `./parser/utils/` - Utility classes (UUID generation, validation, etc.)
- `./parser/output/` - PostgreSQL table managers and CSV generation

#### Expected Output

CSV files matching fhir_tables.sql structure:
- `ehr_vendor.csv`
- `organization.csv` (new table)
- `endpoint_instance.csv` 
- `endpoint_instance_to_other_id.csv` (NPI relationships)
- `endpoint_instance_to_payload.csv`
- `data_lineage.csv` (tracking table)
- `field_coverage_log.csv` (data loss tracking)

#### Benefits

1. **Data Stability**: UUID5 ensures consistent identifiers across runs
2. **Traceability**: Original IDs preserved alongside generated UUIDs
3. **Data Governance**: Comprehensive tracking of ignored/unused JSON fields
4. **Maintainability**: Clean OOP design with single responsibility classes
5. **PostgreSQL Ready**: Direct CSV import without additional transformation
