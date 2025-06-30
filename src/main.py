import os
import csv
import random
import tkinter as tk
from datetime import datetime
import json
from collections import defaultdict


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

    def apply_filter(self, data):
        """Apply this dimensional filter to the data"""
        if not self.column or not self.filter_config:
            return data

        filtered = []
        for row in data:
            if self.matches(row):
                filtered.append(row)
        return filtered

    def matches(self, row):
        """Check if a row matches this filter"""
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
            min_val = self.filter_config.get('min')
            max_val = self.filter_config.get('max')
            if min_val is not None and value < min_val:
                return False
            if max_val is not None and value > max_val:
                return False
            return True

        elif self.column_type == ColumnType.DATE:
            if value is None:
                return False
            date_from = self.filter_config.get('from')
            date_to = self.filter_config.get('to')
            if date_from and value < date_from:
                return False
            if date_to and value > date_to:
                return False
            return True

        return True

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
        # Same logic as DimensionalFilter.matches
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
            min_val = self.filter_config.get('min')
            max_val = self.filter_config.get('max')
            if min_val is not None and value < min_val:
                return False
            if max_val is not None and value > max_val:
                return False
            return True

        elif self.column_type == ColumnType.DATE:
            if value is None:
                return False
            date_from = self.filter_config.get('from')
            date_to = self.filter_config.get('to')
            if date_from and value < date_from:
                return False
            if date_to and value > date_to:
                return False
            return True

        return True

    def get_description(self):
        """Get description of the rule criteria"""
        # Similar to DimensionalFilter but simpler
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
    """Handles all data operations and business logic"""

    def __init__(self):
        # Data storage
        self.data = []
        self.filtered_data = []
        self.column_names = []
        self.column_types = {}
        self.global_filters = []  # List of DimensionalFilter objects
        self.sampling_rules = []  # List of SamplingRule objects
        self.results = []

        # CSV settings
        self.encoding = 'utf-8'
        self.filename = None

        # Make ColumnType accessible
        self.ColumnType = ColumnType

    def detect_column_type(self, values):
        """Detect the type of a column based on its values"""
        if not values:
            return ColumnType.UNKNOWN

        # Try to parse as dates
        date_formats = [
            '%d-%m-%Y', '%d/%m/%Y', '%d.%m.%Y',  # European formats
            '%Y-%m-%d', '%Y/%m/%d', '%Y.%m.%d',  # ISO formats
            '%m/%d/%Y', '%m-%d-%Y', '%m.%d.%Y',  # US formats
        ]
        date_count = 0
        for value in values[:20]:  # Check first 20 values
            if not value:
                continue
            for fmt in date_formats:
                try:
                    datetime.strptime(value, fmt)
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
                # Try both comma and dot as decimal separator
                float(value.replace(',', '.').replace(' ', ''))
                number_count += 1
            except:
                pass

        if number_count > len(values[:20]) * 0.5:  # More than 50% parsed as numbers
            return ColumnType.NUMBER

        return ColumnType.TEXT

    def parse_value(self, value, col_type):
        """Parse a value based on its column type"""
        if not value:
            return None

        if col_type == ColumnType.NUMBER:
            try:
                # Remove spaces and convert comma to dot
                return float(value.replace(',', '.').replace(' ', ''))
            except:
                return None
        elif col_type == ColumnType.DATE:
            date_formats = [
                '%d-%m-%Y', '%d/%m/%Y', '%d.%m.%Y',  # European formats
                '%Y-%m-%d', '%Y/%m/%d', '%Y.%m.%d',  # ISO formats
                '%m/%d/%Y', '%m-%d-%Y', '%m.%d.%Y',  # US formats
            ]
            for fmt in date_formats:
                try:
                    return datetime.strptime(value, fmt)
                except:
                    pass
            return None
        else:
            return value

    def load_csv(self, filename, delimiter):
        """Load data from CSV file"""
        self.filename = filename

        # Read file and detect structure
        with open(filename, 'r', encoding=self.encoding) as file:
            # Read a sample to detect column types
            sample_reader = csv.DictReader(file, delimiter=delimiter)
            sample_data = []
            for i, row in enumerate(sample_reader):
                sample_data.append(row)
                if i >= 100:  # Read up to 100 rows for type detection
                    break

            if not sample_data:
                raise ValueError("No data found in file")

            # Get column names
            self.column_names = list(sample_data[0].keys())

            # Detect column types
            self.column_types = {}
            for column in self.column_names:
                values = [row[column] for row in sample_data]
                self.column_types[column] = self.detect_column_type(values)

        # Now read the full file with proper parsing
        self.data = []
        with open(filename, 'r', encoding=self.encoding) as file:
            reader = csv.DictReader(file, delimiter=delimiter)
            for row in reader:
                parsed_row = {}
                for column in self.column_names:
                    parsed_row[column] = self.parse_value(
                        row[column],
                        self.column_types[column]
                    )
                self.data.append(parsed_row)

        self.filtered_data = self.data.copy()

    def get_filename(self):
        """Get the base filename"""
        return os.path.basename(self.filename) if self.filename else "No file selected"

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
        """Apply all global filters to the data"""
        # Start with all data
        self.filtered_data = self.data.copy()

        # Apply each filter in sequence (AND logic)
        for filter_obj in self.global_filters:
            self.filtered_data = filter_obj.apply_filter(self.filtered_data)

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
        """Save filters and rules to a JSON file"""
        # Prepare global filters data
        global_filters_data = []
        for filter_obj in self.global_filters:
            filter_dict = filter_obj.to_dict()
            # Convert datetime objects to strings
            if filter_obj.column_type == ColumnType.DATE:
                if filter_dict['filter_config'].get('from'):
                    filter_dict['filter_config']['from'] = filter_dict['filter_config']['from'].strftime('%Y-%m-%d')
                if filter_dict['filter_config'].get('to'):
                    filter_dict['filter_config']['to'] = filter_dict['filter_config']['to'].strftime('%Y-%m-%d')
            global_filters_data.append(filter_dict)

        # Prepare sampling rules data
        rules_data = []
        for rule in self.sampling_rules:
            rule_dict = rule.to_dict()
            # Convert datetime objects to strings
            if rule.column_type == ColumnType.DATE:
                if rule_dict['filter_config'].get('from'):
                    rule_dict['filter_config']['from'] = rule_dict['filter_config']['from'].strftime('%Y-%m-%d')
                if rule_dict['filter_config'].get('to'):
                    rule_dict['filter_config']['to'] = rule_dict['filter_config']['to'].strftime('%Y-%m-%d')
            rules_data.append(rule_dict)

        save_data = {
            'column_types': self.column_types,
            'global_filters': global_filters_data,
            'sampling_rules': rules_data
        }

        with open(filename, 'w') as f:
            json.dump(save_data, f, indent=2)

    def load_configuration(self, filename):
        """Load filters and rules from a JSON file"""
        with open(filename, 'r') as f:
            save_data = json.load(f)

        saved_column_types = save_data.get('column_types', {})

        # Load global filters
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
                            row[col] = value.strftime('%d-%m-%Y')
                        else:
                            row[col] = str(value)
                    writer.writerow(row)

        return len(results_by_rule)


def main():
    """Main entry point"""
    # Import UI module
    from ui_tkinter import HybridSampleTestingApp

    # Create root window
    root = tk.Tk()

    # Create data handler
    data_handler = DataHandler()

    # Create and run app
    app = HybridSampleTestingApp(root, data_handler)
    root.mainloop()


if __name__ == "__main__":
    main()
