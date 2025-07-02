# data_handler_db.py
import os
import csv
import random
import logging
from datetime import datetime
import json
from collections import defaultdict
from typing import Optional, List, Dict, Any
from database import SamplingDatabase

# Import the original classes
from sample_testing_main import ColumnType, DimensionalFilter, SamplingRule, DataHandler

# Set up logging
log = logging.getLogger(__name__)


class DatabaseDataHandler(DataHandler):
    """
    Extended DataHandler that supports both CSV files and database operations.
    Maintains compatibility with the original DataHandler interface.
    """

    def __init__(self, db_path: str = "./data/sampling.db"):
        """Initialize with database support."""
        super().__init__()

        # Database connection
        self.db = SamplingDatabase.get_instance(db_path)

        # Current dataset info
        self.current_dataset_id = None
        self.current_dataset_name = None
        self.current_table_name = None

    def load_csv(self, filename, delimiter):
        """
        Load data from CSV file. This now imports the CSV into the database.

        :param filename: Path to CSV file
        :param delimiter: CSV delimiter
        """
        self.filename = filename
        dataset_name = os.path.splitext(os.path.basename(filename))[0]

        # Check if dataset already exists
        existing_dataset = self.db.get_dataset_info(dataset_name)
        if existing_dataset:
            # Load existing dataset from database
            self.load_from_database(dataset_name)
            return

        # Import CSV to database
        log.info(f"Importing CSV file to database: {filename}")

        # First, load data using parent method to detect types
        super().load_csv(filename, delimiter)

        # Create dataset in database
        columns = [(col, self.column_types[col]) for col in self.column_names]
        self.current_table_name = self.db.create_dataset_table(dataset_name, columns)

        # Get dataset ID
        dataset_info = self.db.get_dataset_info(dataset_name)
        self.current_dataset_id = dataset_info['id']
        self.current_dataset_name = dataset_name

        # Insert data into database
        self.db.insert_data_rows(self.current_table_name, self.column_names, self.data)

        log.info(f"Successfully imported {len(self.data)} rows to database")

    def load_from_database(self, dataset_name: str):
        """
        Load a dataset from the database.

        :param dataset_name: Name of the dataset to load
        """
        dataset_info = self.db.get_dataset_info(dataset_name)
        if not dataset_info:
            raise ValueError(f"Dataset '{dataset_name}' not found in database")

        self.current_dataset_id = dataset_info['id']
        self.current_dataset_name = dataset_name
        self.current_table_name = dataset_info['table_name']
        self.filename = dataset_info.get('original_filename', dataset_name)

        # Load column information
        columns = self.db.get_dataset_columns(self.current_dataset_id)
        self.column_names = [col['column_name'] for col in columns]
        self.column_types = {col['column_name']: col['column_type'] for col in columns}

        # Load data
        self.data = self.db.get_all_data(self.current_table_name)
        self.filtered_data = self.data.copy()

        # Load filters and rules if any exist
        filter_dicts = self.db.load_global_filters(self.current_dataset_id)
        self.global_filters = []
        for filter_dict in filter_dicts:
            filter_obj = DimensionalFilter()
            filter_obj.from_dict(filter_dict)
            self.global_filters.append(filter_obj)

        rule_dicts = self.db.load_sampling_rules(self.current_dataset_id)
        self.sampling_rules = []
        for rule_dict in rule_dicts:
            rule = SamplingRule()
            rule.from_dict(rule_dict)
            self.sampling_rules.append(rule)

        log.info(f"Loaded dataset '{dataset_name}' with {len(self.data)} rows")

    def list_available_datasets(self) -> List[Dict[str, Any]]:
        """
        List all available datasets in the database.

        :return: List of dataset information
        """
        return self.db.list_datasets()

    def save_configuration(self, filename):
        """
        Save filters and rules. This now saves to both file and database.

        :param filename: Path to save configuration file
        """
        # Save to file (original behavior)
        super().save_configuration(filename)

        # Also save to database if we have a current dataset
        if self.current_dataset_id:
            # Convert filters to dict format
            filter_dicts = []
            for filter_obj in self.global_filters:
                filter_dicts.append(filter_obj.to_dict())

            # Convert rules to dict format
            rule_dicts = []
            for rule in self.sampling_rules:
                rule_dicts.append(rule.to_dict())

            # Save to database
            self.db.save_global_filters(self.current_dataset_id, filter_dicts)
            self.db.save_sampling_rules(self.current_dataset_id, rule_dicts)

            # Also save as a named configuration
            config_name = os.path.splitext(os.path.basename(filename))[0]
            self.db.save_sampling_config(
                self.current_dataset_id,
                config_name,
                filter_dicts,
                rule_dicts
            )

            log.info("Configuration saved to database")

    def apply_global_filters(self):
        """
        Apply all global filters to the data.
        For large datasets, this could be optimized to use SQL WHERE clauses.
        """
        if self.current_table_name and len(self.data) > 10000:
            # For large datasets, consider building SQL query
            # This is a future optimization
            pass

        # Use original implementation for now
        super().apply_global_filters()

    def get_unique_values(self, column: str, limit: int = 100) -> List[str]:
        """
        Get unique values for a column. Uses database query for efficiency.

        :param column: Column name
        :param limit: Maximum number of unique values to return
        :return: List of unique values
        """
        if self.current_table_name:
            try:
                safe_column = f'"{column}"'
                query = f"""
                    SELECT DISTINCT {safe_column} 
                    FROM {self.current_table_name} 
                    WHERE {safe_column} IS NOT NULL
                    ORDER BY {safe_column}
                    LIMIT ?
                """
                self.db.cursor.execute(query, (limit,))
                return [row[0] for row in self.db.cursor.fetchall()]
            except Exception as e:
                log.error(f"Error getting unique values: {e}")

        # Fallback to memory-based approach
        unique_values = sorted(set(str(row.get(column, '')) for row in self.data
                                   if row.get(column) is not None))[:limit]
        return unique_values

    def export_results(self, filename, delimiter):
        """
        Export sample results. Also saves results to database for history.

        :param filename: Export filename
        :param delimiter: CSV delimiter
        """
        # Export to file (original behavior)
        super().export_results(filename, delimiter)

        # Save results to database for history tracking
        if self.current_dataset_id and self.results:
            try:
                # Group results by rule
                results_by_rule = defaultdict(list)
                for result in self.results:
                    rule_name = result['_rule_name']
                    # Remove the internal rule name before storing
                    clean_result = {k: v for k, v in result.items() if k != '_rule_name'}
                    results_by_rule[rule_name].append(clean_result)

                # Store each rule's results
                for rule_name, rule_results in results_by_rule.items():
                    self.db.cursor.execute("""
                                           INSERT INTO sample_results (dataset_id, rule_name, sample_data)
                                           VALUES (?, ?, ?)
                                           """, (
                                               self.current_dataset_id,
                                               rule_name,
                                               json.dumps(rule_results, default=str)  # Convert dates to strings
                                           ))

                self.db._conn.commit()
                log.info("Sample results saved to database history")

            except Exception as e:
                log.error(f"Error saving results to database: {e}")

    def close(self):
        """Close database connection."""
        if hasattr(self, 'db'):
            self.db.close()


class DatabaseAwareApp:
    """
    Wrapper class that adds database functionality to the UI.
    This can be used to enhance the existing UI with database features.
    """

    def __init__(self, data_handler: DatabaseDataHandler):
        self.data_handler = data_handler

    def show_dataset_selector(self, parent):
        """
        Show a dialog to select from available datasets.
        This would be called from the UI to let users choose a dataset.
        """
        import tkinter as tk
        from tkinter import ttk

        datasets = self.data_handler.list_available_datasets()

        if not datasets:
            return None

        # Create selection dialog
        dialog = tk.Toplevel(parent)
        dialog.title("Select Dataset")
        dialog.geometry("400x300")

        # Dataset list
        ttk.Label(dialog, text="Available Datasets:", font=('TkDefaultFont', 10, 'bold')).pack(pady=10)

        # Create listbox with datasets
        listbox = tk.Listbox(dialog, height=10)
        listbox.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)

        for ds in datasets:
            display_text = f"{ds['name']} ({ds['row_count']} rows)"
            listbox.insert(tk.END, display_text)

        selected_dataset = None

        def on_select():
            nonlocal selected_dataset
            selection = listbox.curselection()
            if selection:
                selected_dataset = datasets[selection[0]]['name']
                dialog.destroy()

        # Buttons
        button_frame = ttk.Frame(dialog)
        button_frame.pack(pady=10)

        ttk.Button(button_frame, text="Select", command=on_select).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Cancel", command=dialog.destroy).pack(side=tk.LEFT, padx=5)

        dialog.wait_window()
        return selected_dataset