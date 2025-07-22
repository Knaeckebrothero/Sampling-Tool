import os
import csv
import random
import tkinter as tk
from datetime import datetime
import json
import logging
from collections import defaultdict
from typing import List, Any, Tuple, Optional
from database import Database
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class ColumnType:
    """Enum for column data types"""
    TEXT = "text"
    NUMBER = "number"
    DATE = "date"
    UNKNOWN = "unknown"


class DimensionalFilter:
    """A global filter for a single column (dimension)"""
    def __init__(self, column="", column_type=ColumnType.TEXT):
        self.column = column
        self.column_type = column_type
        self.filter_config = {}

    def to_dict(self):
        return {
            'column': self.column,
            'column_type': self.column_type,
            'filter_config': self.filter_config
        }

    def from_dict(self, data):
        self.column = data.get('column', '')
        self.column_type = data.get('column_type', ColumnType.TEXT)
        self.filter_config = data.get('filter_config', {})
        return self

    def to_sql_where(self) -> Tuple[str, List[Any]]:
        """Convert filter to SQL WHERE clause and parameters."""
        if not self.column or not self.filter_config:
            return "", []

        params = []

        if self.column_type == ColumnType.TEXT:
            filter_type = self.filter_config.get('type', 'equals')
            if filter_type == 'equals':
                values = self.filter_config.get('values', [])
                if values:
                    placeholders = ','.join('?' * len(values))
                    params.extend(values)
                    return f"{self.column} IN ({placeholders})", params
            elif filter_type == 'contains':
                pattern = self.filter_config.get('pattern', '')
                if pattern:
                    params.append(f"%{pattern}%")
                    return f"{self.column} LIKE ?", params

        elif self.column_type == ColumnType.NUMBER:
            clauses = []
            min_val = self.filter_config.get('min')
            max_val = self.filter_config.get('max')

            if min_val is not None:
                clauses.append(f"CAST({self.column} AS REAL) >= ?")
                params.append(min_val)
            if max_val is not None:
                clauses.append(f"CAST({self.column} AS REAL) <= ?")
                params.append(max_val)

            if clauses:
                return " AND ".join(clauses), params

        elif self.column_type == ColumnType.DATE:
            clauses = []
            date_from = self.filter_config.get('from')
            date_to = self.filter_config.get('to')

            if date_from:
                clauses.append(f"{self.column} >= ?")
                params.append(date_from.strftime('%Y-%m-%d'))
            if date_to:
                clauses.append(f"{self.column} <= ?")
                params.append(date_to.strftime('%Y-%m-%d'))

            if clauses:
                return " AND ".join(clauses), params

        return "", []

    def get_description(self):
        """Get a human-readable description of this filter"""
        if not self.filter_config:
            return f"{self.column}: No filter"

        if self.column_type == ColumnType.TEXT:
            filter_type = self.filter_config.get('type', 'equals')
            if filter_type == 'equals':
                values = self.filter_config.get('values', [])
                if values:
                    return f"{self.column} = {', '.join(values[:3])}{'...' if len(values) > 3 else ''}"
            elif filter_type == 'contains':
                pattern = self.filter_config.get('pattern', '')
                if pattern:
                    return f"{self.column} contains '{pattern}'"

        elif self.column_type == ColumnType.NUMBER:
            parts = []
            if self.filter_config.get('min') is not None:
                parts.append(f">= {self.filter_config['min']:,.2f}")
            if self.filter_config.get('max') is not None:
                parts.append(f"<= {self.filter_config['max']:,.2f}")
            if parts:
                return f"{self.column} {' and '.join(parts)}"

        elif self.column_type == ColumnType.DATE:
            parts = []
            if self.filter_config.get('from'):
                parts.append(f"from {self.filter_config['from'].strftime('%d-%m-%Y')}")
            if self.filter_config.get('to'):
                parts.append(f"to {self.filter_config['to'].strftime('%d-%m-%Y')}")
            if parts:
                return f"{self.column} {' '.join(parts)}"

        return f"{self.column}: No filter"


class SamplingRule:
    """A sampling rule with specific quota requirements"""
    def __init__(self, name="", column="", column_type=ColumnType.TEXT):
        self.name = name
        self.column = column
        self.column_type = column_type
        self.filter_config = {}
        self.sample_count = 5

    def to_dict(self):
        return {
            'name': self.name,
            'column': self.column,
            'column_type': self.column_type,
            'filter_config': self.filter_config,
            'sample_count': self.sample_count
        }

    def from_dict(self, data):
        self.name = data.get('name', '')
        self.column = data.get('column', '')
        self.column_type = data.get('column_type', ColumnType.TEXT)
        self.filter_config = data.get('filter_config', {})
        self.sample_count = data.get('sample_count', 5)
        return self

    def matches(self, row):
        """Check if a row matches this sampling rule"""
        value = row.get(self.column)

        if self.column_type == ColumnType.TEXT:
            filter_type = self.filter_config.get('type', 'equals')
            if filter_type == 'equals':
                values = self.filter_config.get('values', [])
                return not values or str(value) in values
            elif filter_type == 'contains':
                pattern = self.filter_config.get('pattern', '')
                return not pattern or pattern.lower() in str(value).lower()

        elif self.column_type == ColumnType.NUMBER:
            if value is None:
                return False
            try:
                num_value = float(value) if isinstance(value, str) else value
                min_val = self.filter_config.get('min')
                max_val = self.filter_config.get('max')
                if min_val is not None and num_value < min_val:
                    return False
                if max_val is not None and num_value > max_val:
                    return False
                return True
            except:
                return False

        elif self.column_type == ColumnType.DATE:
            if value is None:
                return False
            try:
                # Parse date if it's a string
                if isinstance(value, str):
                    date_value = datetime.strptime(value, '%Y-%m-%d')
                else:
                    date_value = value

                date_from = self.filter_config.get('from')
                date_to = self.filter_config.get('to')
                if date_from and date_value < date_from:
                    return False
                if date_to and date_value > date_to:
                    return False
                return True
            except:
                return False

        return True

    def get_description(self):
        """Get description of the rule criteria"""
        if not self.filter_config:
            return "No criteria"

        if self.column_type == ColumnType.TEXT:
            filter_type = self.filter_config.get('type', 'equals')
            if filter_type == 'equals':
                values = self.filter_config.get('values', [])
                if values:
                    return f"{self.column} = {', '.join(values[:3])}{'...' if len(values) > 3 else ''}"
            elif filter_type == 'contains':
                pattern = self.filter_config.get('pattern', '')
                if pattern:
                    return f"{self.column} contains '{pattern}'"

        elif self.column_type == ColumnType.NUMBER:
            parts = []
            if self.filter_config.get('min') is not None:
                parts.append(f">= {self.filter_config['min']:,.2f}")
            if self.filter_config.get('max') is not None:
                parts.append(f"<= {self.filter_config['max']:,.2f}")
            if parts:
                return f"{self.column} {' and '.join(parts)}"

        elif self.column_type == ColumnType.DATE:
            parts = []
            if self.filter_config.get('from'):
                parts.append(f"from {self.filter_config['from'].strftime('%d-%m-%Y')}")
            if self.filter_config.get('to'):
                parts.append(f"to {self.filter_config['to'].strftime('%d-%m-%Y')}")
            if parts:
                return f"{self.column} {' '.join(parts)}"

        return "No criteria"


class DataHandler:
    """Handles all data operations using database backend"""

    def __init__(self, db_path: Optional[str] = None):
        # Database connection - use environment variable or provided path
        self.db = Database.get_instance(db_path)

        # Data storage
        self.data = []
        self.filtered_data = []
        self.column_names = []
        self.column_types = {}
        self.global_filters = []  # List of DimensionalFilter objects
        self.sampling_rules = []  # List of SamplingRule objects
        self.results = []

        # Configuration
        self.table_name = "kundenstamm"  # Default to main table
        self.available_tables = self.db.get_production_tables()
        self.current_table = "kundenstamm"
        self.join_config = None  # For joined queries

        # Make ColumnType accessible
        self.ColumnType = ColumnType

        # Load initial data if available
        self._initialize_data()

    def _initialize_data(self):
        """Initialize data from database if available"""
        try:
            # Check if tables exist
            existing_tables = self.db.get_all_tables()
            if not any(table in existing_tables for table in self.available_tables):
                log.info("Production tables not found in database")
                return
                
            # Get column information from current table
            columns = self.db.get_table_columns(self.current_table)
            if columns:
                self.column_names = columns
                self._detect_column_types()

                # Load data
                self.data = self.db.get_all_data(self.current_table)
                self.filtered_data = self.data.copy()

                log.info(f"Loaded {len(self.data)} records from {self.current_table}")
        except Exception as e:
            log.error(f"Error initializing data: {e}")

    def _detect_column_types(self):
        """Detect column types from database schema and sample data"""
        # Get SQL column types
        sql_types = self.db.get_column_info(self.current_table)

        # Get sample data for better type detection
        sample_data = self.db.get_sample_data(self.current_table, 100)

        self.column_types = {}
        for column in self.column_names:
            sql_type = sql_types.get(column, 'TEXT').upper()

            # Map SQL types to our column types
            if 'INT' in sql_type or 'REAL' in sql_type or 'NUMERIC' in sql_type:
                self.column_types[column] = ColumnType.NUMBER
            elif 'DATE' in sql_type or 'TIME' in sql_type:
                self.column_types[column] = ColumnType.DATE
            else:
                # For TEXT columns, check if they contain dates or numbers
                values = [row.get(column) for row in sample_data if row.get(column)]
                detected_type = self.detect_column_type(values)
                self.column_types[column] = detected_type

    def detect_column_type(self, values):
        """Detect the type of a column based on its values"""
        if not values:
            return ColumnType.UNKNOWN

        # Try to parse as dates
        date_formats = [
            '%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%d.%m.%Y',
            '%Y/%m/%d', '%Y.%m.%d', '%m/%d/%Y', '%m-%d-%Y', '%m.%d.%Y',
        ]
        date_count = 0
        for value in values[:20]:  # Check first 20 values
            if not value:
                continue
            for fmt in date_formats:
                try:
                    datetime.strptime(str(value), fmt)
                    date_count += 1
                    break
                except:
                    pass

        if date_count > len(values[:20]) * 0.5:  # More than 50% parsed as dates
            return ColumnType.DATE

        # Try to parse as numbers
        number_count = 0
        for value in values[:20]:
            if not value:
                continue
            try:
                # Handle both European (1.234,56) and US (1,234.56) formats
                val_str = str(value)
                # If it has both . and , determine which is decimal separator
                if '.' in val_str and ',' in val_str:
                    # If comma comes after dot, it's decimal separator
                    if val_str.rindex(',') > val_str.rindex('.'):
                        val_str = val_str.replace('.', '').replace(',', '.')
                    else:
                        # Dot is decimal separator
                        val_str = val_str.replace(',', '')
                elif ',' in val_str:
                    # Only comma - assume it's decimal separator (European)
                    val_str = val_str.replace(',', '.')

                float(val_str.replace(' ', ''))
                number_count += 1
            except:
                pass

        if number_count > len(values[:20]) * 0.5:  # More than 50% parsed as numbers
            return ColumnType.NUMBER

        return ColumnType.TEXT


    def refresh_data(self):
        """Refresh data from database"""
        self._initialize_data()

    def get_filename(self):
        """Get current data source description"""
        return f"Database: {self.current_table} ({len(self.data)} records)"
    
    def set_table(self, table_name: str):
        """Switch to a different table"""
        if table_name in self.available_tables:
            self.current_table = table_name
            self.table_name = table_name
            self._initialize_data()
            # Clear filters and results when switching tables
            self.clear_global_filters()
            self.clear_sampling_rules()
            self.clear_results()
    
    def set_join_config(self, join_tables: list, join_type: str = "inner"):
        """Configure joins between tables"""
        self.join_config = {
            'tables': join_tables,
            'type': join_type
        }
        # Reload data with joins
        self._load_joined_data()
    
    def _load_joined_data(self):
        """Load data using configured joins"""
        if not self.join_config:
            return
            
        try:
            relationships = self.db.get_table_relationships()
            join_conditions = relationships.get(self.current_table, {})
            
            self.data = self.db.get_joined_data(
                base_table=self.current_table,
                join_tables=self.join_config['tables'],
                join_conditions=join_conditions
            )
            
            # Update column names to include all joined columns
            if self.data:
                self.column_names = list(self.data[0].keys())
                self._detect_column_types()
                self.filtered_data = self.data.copy()
                
            log.info(f"Loaded {len(self.data)} records with joins")
        except Exception as e:
            log.error(f"Error loading joined data: {e}")
            self.join_config = None

    def clear_filters_and_rules(self):
        """Clear all filters and rules"""
        self.global_filters = []
        self.sampling_rules = []

    def get_available_filter_columns(self, exclude_filter=None):
        """Get columns that don't already have filters"""
        filtered_columns = [f.column for f in self.global_filters if f != exclude_filter]
        return [col for col in self.column_names if col not in filtered_columns]

    def add_global_filter(self, filter_obj):
        """Add a global filter"""
        self.global_filters.append(filter_obj)

    def update_global_filter(self, index, filter_obj):
        """Update an existing global filter"""
        self.global_filters[index] = filter_obj

    def delete_global_filter(self, index):
        """Delete a global filter"""
        del self.global_filters[index]

    def clear_global_filters(self):
        """Clear all global filters"""
        self.global_filters = []

    def apply_global_filters(self):
        """Apply all global filters using SQL queries"""
        try:
            # Build WHERE clause from all filters
            where_clauses = []
            all_params = []

            for filter_obj in self.global_filters:
                clause, params = filter_obj.to_sql_where()
                if clause:
                    where_clauses.append(f"({clause})")
                    all_params.extend(params)

            # Get filtered data from database
            if where_clauses:
                where_clause = " AND ".join(where_clauses)
                self.filtered_data = self.db.get_filtered_data(
                    self.table_name, where_clause, tuple(all_params)
                )
            else:
                self.filtered_data = self.data.copy()

            log.info(f"Applied filters: {len(self.filtered_data)} records match")

        except Exception as e:
            log.error(f"Error applying filters: {e}")
            self.filtered_data = self.data.copy()

    def add_sampling_rule(self, rule):
        """Add a sampling rule"""
        self.sampling_rules.append(rule)

    def update_sampling_rule(self, index, rule):
        """Update an existing sampling rule"""
        self.sampling_rules[index] = rule

    def delete_sampling_rule(self, index):
        """Delete a sampling rule"""
        del self.sampling_rules[index]

    def clear_sampling_rules(self):
        """Clear all sampling rules"""
        self.sampling_rules = []

    def count_available_for_rule(self, rule):
        """Count how many records match a specific rule"""
        count = 0
        for row in self.filtered_data:
            if rule.matches(row):
                count += 1
        return count

    def generate_stratified_sample(self, progress_callback=None):
        """Generate stratified sample based on rules"""
        # Clear previous results
        self.results = []

        # Track which records have been sampled to avoid duplicates
        sampled_indices = set()

        # Process each sampling rule
        rule_results = []
        for i, rule in enumerate(self.sampling_rules):
            if progress_callback:
                progress_callback(i + 1, len(self.sampling_rules))

            # Find matching records that haven't been sampled yet
            matching_records = []
            for idx, row in enumerate(self.filtered_data):
                if idx not in sampled_indices and rule.matches(row):
                    matching_records.append((idx, row))

            # Sample from matching records
            sample_size = min(rule.sample_count, len(matching_records))

            if sample_size > 0:
                sampled = random.sample(matching_records, sample_size)

                for idx, row in sampled:
                    sampled_indices.add(idx)
                    result = row.copy()
                    result['_rule_name'] = rule.name
                    self.results.append(result)

                rule_results.append(f"{rule.name}: {sample_size} samples")
            else:
                rule_results.append(f"{rule.name}: 0 samples (no matches)")

        return rule_results

    def clear_results(self):
        """Clear sampling results"""
        self.results = []

    def save_configuration(self, filename):
        """Save filters and rules to JSON file"""
        try:
            # Prepare configuration data
            config_data = {
                'column_types': self.column_types,
                'global_filters': [],
                'sampling_rules': []
            }

            # Convert filters
            for filter_obj in self.global_filters:
                filter_dict = filter_obj.to_dict()
                # Convert datetime objects to strings
                if filter_obj.column_type == ColumnType.DATE:
                    if filter_dict['filter_config'].get('from'):
                        filter_dict['filter_config']['from'] = filter_dict['filter_config']['from'].strftime('%Y-%m-%d')
                    if filter_dict['filter_config'].get('to'):
                        filter_dict['filter_config']['to'] = filter_dict['filter_config']['to'].strftime('%Y-%m-%d')
                config_data['global_filters'].append(filter_dict)

            # Convert rules
            for rule in self.sampling_rules:
                rule_dict = rule.to_dict()
                # Convert datetime objects to strings
                if rule.column_type == ColumnType.DATE:
                    if rule_dict['filter_config'].get('from'):
                        rule_dict['filter_config']['from'] = rule_dict['filter_config']['from'].strftime('%Y-%m-%d')
                    if rule_dict['filter_config'].get('to'):
                        rule_dict['filter_config']['to'] = rule_dict['filter_config']['to'].strftime('%Y-%m-%d')
                config_data['sampling_rules'].append(rule_dict)

            # Save to JSON file
            with open(filename, 'w') as f:
                json.dump(config_data, f, indent=2)

            log.info(f"Configuration saved to {filename}")

        except Exception as e:
            log.error(f"Error saving configuration: {e}")
            raise

    def load_configuration(self, filename):
        """Load filters and rules from JSON file"""
        try:
            # Load from JSON file
            with open(filename, 'r') as f:
                save_data = json.load(f)

            # Load filters
            self.global_filters = []
            for filter_dict in save_data.get('global_filters', []):
                if filter_dict['column'] in self.column_types:
                    filter_obj = DimensionalFilter()
                    # Convert date strings back to datetime objects
                    if filter_dict['column_type'] == ColumnType.DATE:
                        if filter_dict['filter_config'].get('from'):
                            filter_dict['filter_config']['from'] = datetime.strptime(
                                filter_dict['filter_config']['from'], '%Y-%m-%d')
                        if filter_dict['filter_config'].get('to'):
                            filter_dict['filter_config']['to'] = datetime.strptime(
                                filter_dict['filter_config']['to'], '%Y-%m-%d')
                    filter_obj.from_dict(filter_dict)
                    self.global_filters.append(filter_obj)

            # Load sampling rules
            self.sampling_rules = []
            for rule_dict in save_data.get('sampling_rules', []):
                if rule_dict['column'] in self.column_types:
                    rule = SamplingRule()
                    # Convert date strings back to datetime objects
                    if rule_dict['column_type'] == ColumnType.DATE:
                        if rule_dict['filter_config'].get('from'):
                            rule_dict['filter_config']['from'] = datetime.strptime(
                                rule_dict['filter_config']['from'], '%Y-%m-%d')
                        if rule_dict['filter_config'].get('to'):
                            rule_dict['filter_config']['to'] = datetime.strptime(
                                rule_dict['filter_config']['to'], '%Y-%m-%d')
                    rule.from_dict(rule_dict)
                    self.sampling_rules.append(rule)

            return len(self.global_filters), len(self.sampling_rules)

        except Exception as e:
            log.error(f"Error loading configuration: {e}")
            raise

    def export_results(self, filename, delimiter):
        """Export all sample results to CSV"""
        with open(filename, 'w', newline='', encoding='utf-8') as file:
            # Write with rule column first, then data columns
            fieldnames = ['rule'] + self.column_names
            writer = csv.DictWriter(file, fieldnames=fieldnames, delimiter=delimiter)

            writer.writeheader()

            for result in self.results:
                row = {'rule': result['_rule_name']}
                for col in self.column_names:
                    value = result.get(col)
                    if value is None:
                        row[col] = ''
                    elif self.column_types[col] == ColumnType.NUMBER:
                        row[col] = str(value).replace('.', ',')
                    elif self.column_types[col] == ColumnType.DATE:
                        if isinstance(value, str):
                            row[col] = value
                        else:
                            row[col] = value.strftime('%d-%m-%Y')
                    else:
                        row[col] = str(value)
                writer.writerow(row)

    def export_by_rule(self, directory, delimiter):
        """Export results grouped by rule to separate files"""
        # Group results by rule
        results_by_rule = defaultdict(list)
        for result in self.results:
            results_by_rule[result['_rule_name']].append(result)

        # Export each rule's results
        for rule_name, rule_results in results_by_rule.items():
            # Create safe filename
            safe_name = "".join(c for c in rule_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
            filename = os.path.join(directory, f"{safe_name}.csv")

            with open(filename, 'w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=self.column_names, delimiter=delimiter)

                writer.writeheader()

                for result in rule_results:
                    row = {}
                    for col in self.column_names:
                        value = result.get(col)
                        if value is None:
                            row[col] = ''
                        elif self.column_types[col] == ColumnType.NUMBER:
                            row[col] = str(value).replace('.', ',')
                        elif self.column_types[col] == ColumnType.DATE:
                            if isinstance(value, str):
                                row[col] = value
                            else:
                                row[col] = value.strftime('%d-%m-%Y')
                        else:
                            row[col] = str(value)
                    writer.writerow(row)

        return len(results_by_rule)


def main():
    """Main entry point"""
    # Import UI module
    from ui_tkinter import SimpleSampleTestingApp

    # Create root window
    root = tk.Tk()

    # Create data handler with database backend
    data_handler = DataHandler()

    # Create and run app
    app = SimpleSampleTestingApp(root, data_handler)
    root.mainloop()


if __name__ == "__main__":
    main()
