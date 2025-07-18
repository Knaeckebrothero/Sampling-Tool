import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from datetime import datetime
import random
import json
from collections import defaultdict


class SimpleSampleTestingApp:
    def __init__(self, root, data_handler):
        self.root = root
        self.root.title("Simple Stratified Sampling Tool")
        self.root.geometry("1200x700")

        # Reference to data handler
        self.data_handler = data_handler

        # Configure styles
        style = ttk.Style()
        style.configure('Accent.TButton', font=('TkDefaultFont', 11, 'bold'))


        # Create main frames
        self.create_widgets()

    def create_widgets(self):
        # Create notebook for tabs
        self.notebook = ttk.Notebook(self.root)
        self.notebook.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=5, pady=5)

        # Tab 1: Data Loading
        self.data_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.data_tab, text="Data Loading")
        self.create_data_tab()

        # Tab 2: Global Filters
        self.filters_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.filters_tab, text="Global Filters")
        self.create_filters_tab()

        # Tab 3: Sampling Rules
        self.rules_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.rules_tab, text="Sampling Rules")
        self.create_rules_tab()

        # Tab 4: Results
        self.results_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.results_tab, text="Sample Results")
        self.create_results_tab()

        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        # Initialize data after all widgets are created
        self.load_file()

    def create_data_tab(self):
        # Database connection frame
        db_frame = ttk.Frame(self.data_tab, padding="10")
        db_frame.grid(row=0, column=0, sticky=(tk.W, tk.E))

        ttk.Label(db_frame, text="Data Source:").grid(row=0, column=0, sticky=tk.W)
        self.file_label = ttk.Label(db_frame, text="Connecting to database...", relief=tk.SUNKEN, width=50)
        self.file_label.grid(row=0, column=1, padx=5, sticky=(tk.W, tk.E))
        ttk.Button(db_frame, text="Refresh Data", command=self.load_file).grid(row=0, column=2)
        
        # Table selector
        ttk.Label(db_frame, text="Active Table:").grid(row=1, column=0, sticky=tk.W, pady=(5, 0))
        self.table_var = tk.StringVar(value="kundenstamm")
        self.table_combo = ttk.Combobox(db_frame, textvariable=self.table_var,
                                        values=self.data_handler.available_tables, 
                                        state="readonly", width=30)
        self.table_combo.grid(row=1, column=1, padx=5, pady=(5, 0), sticky=tk.W)
        self.table_combo.bind('<<ComboboxSelected>>', self.on_table_changed)
        
        # Join tables button
        ttk.Button(db_frame, text="Configure Joins...", command=self.configure_joins).grid(row=1, column=2, pady=(5, 0))


        # Column info frame
        info_frame = ttk.LabelFrame(self.data_tab, text="Detected Columns", padding="10")
        info_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=10, pady=5)

        # Column list
        self.column_listbox = tk.Listbox(info_frame, height=10)
        column_scroll = ttk.Scrollbar(info_frame, orient=tk.VERTICAL, command=self.column_listbox.yview)
        self.column_listbox.configure(yscrollcommand=column_scroll.set)

        self.column_listbox.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        column_scroll.grid(row=0, column=1, sticky=(tk.N, tk.S))

        # Data preview
        preview_frame = ttk.LabelFrame(self.data_tab, text="Data Preview", padding="10")
        preview_frame.grid(row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=10, pady=5)

        # Create preview tree
        self.preview_tree = ttk.Treeview(preview_frame, show='headings', height=10)
        preview_scroll = ttk.Scrollbar(preview_frame, orient=tk.VERTICAL, command=self.preview_tree.yview)
        self.preview_tree.configure(yscrollcommand=preview_scroll.set)

        self.preview_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        preview_scroll.grid(row=0, column=1, sticky=(tk.N, tk.S))

        # Info labels
        self.info_label = ttk.Label(preview_frame, text="")
        self.info_label.grid(row=1, column=0, columnspan=2, pady=5)

        # Configure grid
        self.data_tab.columnconfigure(0, weight=1)
        self.data_tab.rowconfigure(2, weight=1)
        info_frame.columnconfigure(0, weight=1)
        info_frame.rowconfigure(0, weight=1)
        preview_frame.columnconfigure(0, weight=1)
        preview_frame.rowconfigure(0, weight=1)


    def create_filters_tab(self):
        # Instructions
        inst_frame = ttk.Frame(self.filters_tab, padding="10")
        inst_frame.grid(row=0, column=0, sticky=(tk.W, tk.E))

        instructions = ttk.Label(inst_frame,
                                 text="Global filters apply to ALL data before sampling rules are applied.\n" +
                                      "Example: Set Year = 2024 here, then all sampling rules will only consider 2024 data.",
                                 font=('TkDefaultFont', 9, 'italic'),
                                 wraplength=800)
        instructions.grid(row=0, column=0, pady=5)

        # Button frame
        button_frame = ttk.Frame(self.filters_tab, padding="10")
        button_frame.grid(row=1, column=0, sticky=(tk.W, tk.E))

        ttk.Button(button_frame, text="Add Global Filter", command=self.add_global_filter).grid(row=0, column=0, padx=5)
        ttk.Button(button_frame, text="Edit Filter", command=self.edit_global_filter).grid(row=0, column=1, padx=5)
        ttk.Button(button_frame, text="Delete Filter", command=self.delete_global_filter).grid(row=0, column=2, padx=5)
        ttk.Button(button_frame, text="Clear All", command=self.clear_global_filters).grid(row=0, column=3, padx=5)

        # Filters list
        filters_frame = ttk.LabelFrame(self.filters_tab, text="Active Global Filters", padding="10")
        filters_frame.grid(row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=10, pady=5)

        columns = ('Column', 'Type', 'Filter')
        self.filters_tree = ttk.Treeview(filters_frame, columns=columns, show='headings', height=10)

        self.filters_tree.heading('Column', text='Column')
        self.filters_tree.heading('Type', text='Data Type')
        self.filters_tree.heading('Filter', text='Filter Criteria')

        self.filters_tree.column('Column', width=200)
        self.filters_tree.column('Type', width=100)
        self.filters_tree.column('Filter', width=400)

        filters_scroll = ttk.Scrollbar(filters_frame, orient=tk.VERTICAL, command=self.filters_tree.yview)
        self.filters_tree.configure(yscrollcommand=filters_scroll.set)

        self.filters_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        filters_scroll.grid(row=0, column=1, sticky=(tk.N, tk.S))

        # Status frame
        status_frame = ttk.Frame(self.filters_tab, padding="10")
        status_frame.grid(row=3, column=0, sticky=(tk.W, tk.E))

        self.global_filtered_label = ttk.Label(status_frame, text="Total records: 0 | After global filters: 0")
        self.global_filtered_label.grid(row=0, column=0)

        ttk.Button(status_frame, text="🔍 Apply Global Filters",
                   command=self.apply_global_filters).grid(row=0, column=1, padx=20)

        # Configure grid
        self.filters_tab.columnconfigure(0, weight=1)
        self.filters_tab.rowconfigure(2, weight=1)
        filters_frame.columnconfigure(0, weight=1)
        filters_frame.rowconfigure(0, weight=1)

    def create_rules_tab(self):
        # Instructions
        inst_frame = ttk.Frame(self.rules_tab, padding="10")
        inst_frame.grid(row=0, column=0, sticky=(tk.W, tk.E))

        instructions = ttk.Label(inst_frame,
                                 text="Sampling rules specify how many samples to take from specific criteria.\n" +
                                      "Example: '5 samples where Legal Form = GmbH', '3 samples where Legal Form = AG'\n" +
                                      "Rules are applied to data that passes global filters.",
                                 font=('TkDefaultFont', 9, 'italic'),
                                 wraplength=800)
        instructions.grid(row=0, column=0, pady=5)

        # Button frame
        button_frame = ttk.Frame(self.rules_tab, padding="10")
        button_frame.grid(row=1, column=0, sticky=(tk.W, tk.E))

        ttk.Button(button_frame, text="Add Sampling Rule", command=self.add_sampling_rule).grid(row=0, column=0, padx=5)
        ttk.Button(button_frame, text="Edit Rule", command=self.edit_sampling_rule).grid(row=0, column=1, padx=5)
        ttk.Button(button_frame, text="Delete Rule", command=self.delete_sampling_rule).grid(row=0, column=2, padx=5)
        ttk.Button(button_frame, text="Clear All", command=self.clear_sampling_rules).grid(row=0, column=3, padx=5)
        ttk.Button(button_frame, text="Save Config", command=self.save_configuration).grid(row=0, column=4, padx=5)
        ttk.Button(button_frame, text="Load Config", command=self.load_configuration).grid(row=0, column=5, padx=5)

        # Rules list
        rules_frame = ttk.LabelFrame(self.rules_tab, text="Sampling Rules with Quotas", padding="10")
        rules_frame.grid(row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=10, pady=5)

        columns = ('Rule Name', 'Criteria', 'Sample Count', 'Available')
        self.rules_tree = ttk.Treeview(rules_frame, columns=columns, show='headings', height=10)

        self.rules_tree.heading('Rule Name', text='Rule Name')
        self.rules_tree.heading('Criteria', text='Sampling Criteria')
        self.rules_tree.heading('Sample Count', text='Samples Required')
        self.rules_tree.heading('Available', text='Available Records')

        self.rules_tree.column('Rule Name', width=200)
        self.rules_tree.column('Criteria', width=350)
        self.rules_tree.column('Sample Count', width=120)
        self.rules_tree.column('Available', width=120)

        rules_scroll = ttk.Scrollbar(rules_frame, orient=tk.VERTICAL, command=self.rules_tree.yview)
        self.rules_tree.configure(yscrollcommand=rules_scroll.set)

        self.rules_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        rules_scroll.grid(row=0, column=1, sticky=(tk.N, tk.S))

        # Generate button
        generate_frame = ttk.Frame(self.rules_tab, padding="10")
        generate_frame.grid(row=3, column=0, sticky=(tk.W, tk.E))

        self.total_required_label = ttk.Label(generate_frame, text="Total samples required: 0")
        self.total_required_label.grid(row=0, column=0, padx=10)

        ttk.Button(generate_frame, text="🎯 Generate Stratified Sample",
                   command=self.generate_stratified_sample,
                   style='Accent.TButton').grid(row=0, column=1, padx=10)

        self.progress_label = ttk.Label(generate_frame, text="")
        self.progress_label.grid(row=0, column=2, padx=10)

        # Configure grid
        self.rules_tab.columnconfigure(0, weight=1)
        self.rules_tab.rowconfigure(2, weight=1)
        rules_frame.columnconfigure(0, weight=1)
        rules_frame.rowconfigure(0, weight=1)

    def create_results_tab(self):
        # Results summary
        summary_frame = ttk.Frame(self.results_tab, padding="10")
        summary_frame.grid(row=0, column=0, sticky=(tk.W, tk.E))

        self.results_summary_label = ttk.Label(summary_frame, text="No sample generated yet")
        self.results_summary_label.grid(row=0, column=0)

        # Results tree
        results_frame = ttk.LabelFrame(self.results_tab, text="Sample Results", padding="10")
        results_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=10, pady=5)

        self.results_tree = ttk.Treeview(results_frame, show='headings', height=15)
        results_scroll = ttk.Scrollbar(results_frame, orient=tk.VERTICAL, command=self.results_tree.yview)
        self.results_tree.configure(yscrollcommand=results_scroll.set)

        self.results_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        results_scroll.grid(row=0, column=1, sticky=(tk.N, tk.S))

        # Export buttons
        export_frame = ttk.Frame(self.results_tab, padding="10")
        export_frame.grid(row=2, column=0, sticky=(tk.W, tk.E))

        ttk.Button(export_frame, text="Export Sample to CSV", command=self.export_results).grid(row=0, column=0, padx=5)
        ttk.Button(export_frame, text="Export by Rule", command=self.export_by_rule).grid(row=0, column=1, padx=5)
        ttk.Button(export_frame, text="Clear Results", command=self.clear_results).grid(row=0, column=2, padx=5)

        # Configure grid
        self.results_tab.columnconfigure(0, weight=1)
        self.results_tab.rowconfigure(1, weight=1)
        results_frame.columnconfigure(0, weight=1)
        results_frame.rowconfigure(0, weight=1)

    def load_file(self):
        # For database version, this refreshes data from database
        try:
            self.data_handler.refresh_data()

            self.file_label.config(text=self.data_handler.get_filename())
            self.update_column_display()
            self.update_preview()
            self.setup_dynamic_trees()

            # Only show success if we have data
            if self.data_handler.data:
                messagebox.showinfo("Success",
                                    f"Connected to database: {len(self.data_handler.data)} records with {len(self.data_handler.column_names)} columns")
            else:
                messagebox.showwarning("No Data",
                                       "Database is empty. Please ensure production database contains data.")

        except Exception as e:
            messagebox.showerror("Error", f"Failed to load data: {str(e)}")

    def update_column_display(self):
        """Update the column list display"""
        self.column_listbox.delete(0, tk.END)

        for column in self.data_handler.column_names:
            col_type = self.data_handler.column_types[column]
            self.column_listbox.insert(tk.END, f"{column} [{col_type}]")

    def update_preview(self):
        """Update the data preview"""
        # Clear existing columns
        for col in self.preview_tree['columns']:
            self.preview_tree.heading(col, text='')
        self.preview_tree['columns'] = ()

        # Set new columns
        self.preview_tree['columns'] = self.data_handler.column_names

        # Configure columns
        for col in self.data_handler.column_names:
            self.preview_tree.heading(col, text=col)
            self.preview_tree.column(col, width=120)

        # Clear existing data
        for item in self.preview_tree.get_children():
            self.preview_tree.delete(item)

        # Add preview data (first 50 rows)
        for row in self.data_handler.data[:50]:
            values = []
            for col in self.data_handler.column_names:
                value = row[col]
                if value is None:
                    values.append('')
                elif self.data_handler.column_types[col] == self.data_handler.ColumnType.NUMBER:
                    try:
                        # Format number with European style (comma as decimal separator)
                        num_val = float(value)
                        values.append(f"{num_val:,.2f}".replace(',', ' ').replace('.', ','))
                    except:
                        values.append(str(value))
                elif self.data_handler.column_types[col] == self.data_handler.ColumnType.DATE:
                    if isinstance(value, str):
                        values.append(value)
                    else:
                        values.append(value.strftime('%d-%m-%Y'))
                else:
                    values.append(str(value))
            self.preview_tree.insert('', tk.END, values=values)

        # Update info
        self.info_label.config(text=f"Showing {min(50, len(self.data_handler.data))} of {len(self.data_handler.data)} records")

    def setup_dynamic_trees(self):
        """Setup the results tree with dynamic columns"""
        # Only setup if results_tree exists (it might not during initialization)
        if hasattr(self, 'results_tree'):
            # Results tree columns: Rule Name + all data columns
            columns = ['Rule'] + self.data_handler.column_names
            self.results_tree['columns'] = columns

            for col in columns:
                self.results_tree.heading(col, text=col)
                self.results_tree.column(col, width=120)

    def add_global_filter(self):
        if not self.data_handler.column_names:
            messagebox.showwarning("Warning", "Please load data first")
            return

        # Get available columns (not already filtered)
        available_columns = self.data_handler.get_available_filter_columns()

        if not available_columns:
            messagebox.showwarning("Warning", "All columns already have global filters")
            return

        dialog = GlobalFilterDialog(self.root, "Add Global Filter",
                                    available_columns, self.data_handler.column_types,
                                    self.data_handler.data, self.data_handler.ColumnType)
        self.root.wait_window(dialog.dialog)

        if dialog.result:
            self.data_handler.add_global_filter(dialog.result)
            self.update_filters_display()
            self.apply_global_filters()

    def edit_global_filter(self):
        selection = self.filters_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a filter to edit")
            return

        index = self.filters_tree.index(selection[0])
        filter_obj = self.data_handler.global_filters[index]

        # For editing, include the current column in available columns
        available_columns = self.data_handler.get_available_filter_columns(exclude_filter=filter_obj)

        dialog = GlobalFilterDialog(self.root, "Edit Global Filter",
                                    available_columns, self.data_handler.column_types,
                                    self.data_handler.data, self.data_handler.ColumnType, filter_obj)
        self.root.wait_window(dialog.dialog)

        if dialog.result:
            self.data_handler.update_global_filter(index, dialog.result)
            self.update_filters_display()
            self.apply_global_filters()

    def delete_global_filter(self):
        selection = self.filters_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a filter to delete")
            return

        index = self.filters_tree.index(selection[0])
        self.data_handler.delete_global_filter(index)
        self.update_filters_display()
        self.apply_global_filters()

    def clear_global_filters(self):
        if self.data_handler.global_filters and messagebox.askyesno("Confirm", "Clear all global filters?"):
            self.data_handler.clear_global_filters()
            self.update_filters_display()
            self.apply_global_filters()

    def update_filters_display(self):
        """Update the filters tree display"""
        for item in self.filters_tree.get_children():
            self.filters_tree.delete(item)

        for filter_obj in self.data_handler.global_filters:
            self.filters_tree.insert('', tk.END, values=(
                filter_obj.column,
                filter_obj.column_type,
                filter_obj.get_description()
            ))

    def apply_global_filters(self):
        """Apply all global filters to the data"""
        if not self.data_handler.data:
            return

        self.data_handler.apply_global_filters()

        # Update count
        self.global_filtered_label.config(
            text=f"Total records: {len(self.data_handler.data)} | After global filters: {len(self.data_handler.filtered_data)}"
        )

        # Update sampling rules to show available counts
        self.update_rules_display()

    def add_sampling_rule(self):
        if not self.data_handler.column_names:
            messagebox.showwarning("Warning", "Please load data first")
            return

        dialog = SamplingRuleDialog(self.root, "Add Sampling Rule",
                                    self.data_handler.column_names, self.data_handler.column_types,
                                    self.data_handler.data, self.data_handler.ColumnType)
        self.root.wait_window(dialog.dialog)

        if dialog.result:
            self.data_handler.add_sampling_rule(dialog.result)
            self.update_rules_display()

    def edit_sampling_rule(self):
        selection = self.rules_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a rule to edit")
            return

        index = self.rules_tree.index(selection[0])
        rule = self.data_handler.sampling_rules[index]

        dialog = SamplingRuleDialog(self.root, "Edit Sampling Rule",
                                    self.data_handler.column_names, self.data_handler.column_types,
                                    self.data_handler.data, self.data_handler.ColumnType, rule)
        self.root.wait_window(dialog.dialog)

        if dialog.result:
            self.data_handler.update_sampling_rule(index, dialog.result)
            self.update_rules_display()

    def delete_sampling_rule(self):
        selection = self.rules_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a rule to delete")
            return

        index = self.rules_tree.index(selection[0])
        self.data_handler.delete_sampling_rule(index)
        self.update_rules_display()

    def clear_sampling_rules(self):
        if self.data_handler.sampling_rules and messagebox.askyesno("Confirm", "Clear all sampling rules?"):
            self.data_handler.clear_sampling_rules()
            self.update_rules_display()

    def update_rules_display(self):
        """Update the rules tree display"""
        for item in self.rules_tree.get_children():
            self.rules_tree.delete(item)

        total_required = 0

        for rule in self.data_handler.sampling_rules:
            # Count available records for this rule
            available = self.data_handler.count_available_for_rule(rule)

            self.rules_tree.insert('', tk.END, values=(
                rule.name,
                rule.get_description(),
                rule.sample_count,
                available
            ))

            total_required += rule.sample_count

        self.total_required_label.config(text=f"Total samples required: {total_required}")

    def generate_stratified_sample(self):
        """Generate stratified sample based on rules"""
        if not self.data_handler.filtered_data:
            messagebox.showwarning("Warning", "No data available. Apply global filters first.")
            return

        if not self.data_handler.sampling_rules:
            messagebox.showwarning("Warning", "No sampling rules defined.")
            return

        # Generate sample with progress callback
        def progress_callback(current, total):
            self.progress_label.config(text=f"Processing rule {current} of {total}...")
            self.root.update()

        rule_results = self.data_handler.generate_stratified_sample(progress_callback)

        self.progress_label.config(text="")

        # Update results display
        self.update_results_display()

        # Switch to results tab
        self.notebook.select(self.results_tab)

        # Show summary
        summary = "\n".join(rule_results)
        messagebox.showinfo("Sampling Complete",
                            f"Generated {len(self.data_handler.results)} total samples:\n\n{summary}")

    def update_results_display(self):
        """Update the results display"""
        # Update summary
        global_filter_summary = " AND ".join(f.get_description() for f in self.data_handler.global_filters) if self.data_handler.global_filters else "None"
        self.results_summary_label.config(
            text=f"Total samples: {len(self.data_handler.results)} | Global filters: {global_filter_summary}"
        )

        # Clear tree
        for item in self.results_tree.get_children():
            self.results_tree.delete(item)

        # Add results
        for result in self.data_handler.results:
            values = [result['_rule_name']]

            for col in self.data_handler.column_names:
                value = result.get(col)
                if value is None:
                    values.append('')
                elif self.data_handler.column_types[col] == self.data_handler.ColumnType.NUMBER:
                    try:
                        # Format number with European style (comma as decimal separator)
                        num_val = float(value)
                        values.append(f"{num_val:,.2f}".replace(',', ' ').replace('.', ','))
                    except:
                        values.append(str(value))
                elif self.data_handler.column_types[col] == self.data_handler.ColumnType.DATE:
                    if isinstance(value, str):
                        values.append(value)
                    else:
                        values.append(value.strftime('%d-%m-%Y'))
                else:
                    values.append(str(value))

            self.results_tree.insert('', tk.END, values=values)

    def save_configuration(self):
        """Save filters and rules to a JSON file"""
        if not self.data_handler.global_filters and not self.data_handler.sampling_rules:
            messagebox.showwarning("Warning", "No configuration to save")
            return

        filename = filedialog.asksaveasfilename(
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )

        if filename:
            try:
                self.data_handler.save_configuration(filename)
                messagebox.showinfo("Success", f"Configuration saved to {filename.split('/')[-1]}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to save configuration: {str(e)}")

    def load_configuration(self):
        """Load filters and rules from a JSON file"""
        filename = filedialog.askopenfilename(
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )

        if filename:
            try:
                loaded_filters, loaded_rules = self.data_handler.load_configuration(filename)

                self.update_filters_display()
                self.update_rules_display()
                self.apply_global_filters()

                messagebox.showinfo("Success",
                                    f"Loaded {loaded_filters} global filters and {loaded_rules} sampling rules")

            except Exception as e:
                messagebox.showerror("Error", f"Failed to load configuration: {str(e)}")

    def export_results(self):
        """Export all sample results to CSV"""
        if not self.data_handler.results:
            messagebox.showwarning("Warning", "No results to export")
            return

        filename = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )

        if filename:
            try:
                delimiter = ';'  # Default delimiter
                self.data_handler.export_results(filename, delimiter)
                messagebox.showinfo("Success", f"Results exported to {filename.split('/')[-1]}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to export: {str(e)}")

    def export_by_rule(self):
        """Export results grouped by rule to separate files"""
        if not self.data_handler.results:
            messagebox.showwarning("Warning", "No results to export")
            return

        # Ask for directory
        directory = filedialog.askdirectory(title="Select directory for export")
        if not directory:
            return

        try:
            delimiter = ';'  # Default delimiter
            file_count = self.data_handler.export_by_rule(directory, delimiter)
            messagebox.showinfo("Success", f"Exported {file_count} files to {directory}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to export: {str(e)}")

    def clear_results(self):
        """Clear all results"""
        if self.data_handler.results and messagebox.askyesno("Confirm", "Clear all results?"):
            self.data_handler.clear_results()
            self.update_results_display()
            self.results_summary_label.config(text="Results cleared")
    
    def on_table_changed(self, event=None):
        """Handle table selection change"""
        selected_table = self.table_var.get()
        if selected_table != self.data_handler.current_table:
            if self.data_handler.results:
                if not messagebox.askyesno("Confirm", "Changing tables will clear current results. Continue?"):
                    # Reset combo to current table
                    self.table_var.set(self.data_handler.current_table)
                    return
            
            # Change table
            self.data_handler.set_table(selected_table)
            self.load_file()
            messagebox.showinfo("Success", f"Switched to table: {selected_table}")
    
    def configure_joins(self):
        """Open dialog to configure table joins"""
        dialog = JoinConfigDialog(self.root, self.data_handler)
        self.root.wait_window(dialog.dialog)
        
        if dialog.result:
            # Apply join configuration
            self.data_handler.set_join_config(dialog.result['tables'], dialog.result['type'])
            self.load_file()
            messagebox.showinfo("Success", "Join configuration applied")


class JoinConfigDialog:
    """Dialog for configuring table joins"""
    def __init__(self, parent, data_handler):
        self.result = None
        self.data_handler = data_handler
        
        # Create dialog
        self.dialog = tk.Toplevel(parent)
        self.dialog.title("Configure Table Joins")
        self.dialog.geometry("400x300")
        self.dialog.transient(parent)
        self.dialog.grab_set()
        
        # Available tables (excluding current base table)
        self.available_tables = [t for t in data_handler.available_tables if t != data_handler.current_table]
        
        # Create widgets
        self.create_widgets()
        
    def create_widgets(self):
        # Instructions
        ttk.Label(self.dialog, text=f"Base table: {self.data_handler.current_table}", 
                 font=('TkDefaultFont', 10, 'bold')).grid(row=0, column=0, columnspan=2, padx=10, pady=10)
        
        ttk.Label(self.dialog, text="Select tables to join:").grid(row=1, column=0, columnspan=2, padx=10, pady=5)
        
        # Table selection
        self.table_vars = {}
        row = 2
        for table in self.available_tables:
            var = tk.BooleanVar()
            self.table_vars[table] = var
            ttk.Checkbutton(self.dialog, text=table, variable=var).grid(row=row, column=0, padx=20, sticky=tk.W)
            row += 1
        
        # Join type
        ttk.Label(self.dialog, text="Join type:").grid(row=row, column=0, padx=10, pady=(10, 5), sticky=tk.W)
        self.join_type_var = tk.StringVar(value="inner")
        join_frame = ttk.Frame(self.dialog)
        join_frame.grid(row=row+1, column=0, columnspan=2, padx=20)
        
        ttk.Radiobutton(join_frame, text="Inner Join", variable=self.join_type_var, value="inner").grid(row=0, column=0)
        ttk.Radiobutton(join_frame, text="Left Join", variable=self.join_type_var, value="left").grid(row=0, column=1)
        
        # Buttons
        button_frame = ttk.Frame(self.dialog)
        button_frame.grid(row=row+2, column=0, columnspan=2, pady=20)
        
        ttk.Button(button_frame, text="Apply", command=self.apply_join).grid(row=0, column=0, padx=5)
        ttk.Button(button_frame, text="Cancel", command=self.dialog.destroy).grid(row=0, column=1, padx=5)
        
    def apply_join(self):
        """Apply the join configuration"""
        selected_tables = [table for table, var in self.table_vars.items() if var.get()]
        
        if not selected_tables:
            messagebox.showwarning("Warning", "Please select at least one table to join")
            return
        
        self.result = {
            'tables': selected_tables,
            'type': self.join_type_var.get()
        }
        self.dialog.destroy()


class GlobalFilterDialog:
    """Dialog for creating/editing a global dimensional filter"""
    def __init__(self, parent, title, available_columns, column_types, data, ColumnType, filter_obj=None):
        self.result = None
        self.available_columns = available_columns
        self.column_types = column_types
        self.data = data
        self.ColumnType = ColumnType
        self.tooltip = None

        # Create dialog
        self.dialog = tk.Toplevel(parent)
        self.dialog.title(title)
        self.dialog.geometry("500x500")
        self.dialog.transient(parent)
        self.dialog.grab_set()

        # Initialize with existing filter or create new
        self.filter_obj = filter_obj

        self.create_widgets()

        # Center the dialog
        self.dialog.update_idletasks()
        x = (self.dialog.winfo_screenwidth() // 2) - (self.dialog.winfo_width() // 2)
        y = (self.dialog.winfo_screenheight() // 2) - (self.dialog.winfo_height() // 2)
        self.dialog.geometry(f"+{x}+{y}")

    def create_widgets(self):
        # Column selection
        col_frame = ttk.Frame(self.dialog, padding="10")
        col_frame.grid(row=0, column=0, sticky=(tk.W, tk.E))

        ttk.Label(col_frame, text="Select Column:").grid(row=0, column=0, sticky=tk.W)
        self.column_var = tk.StringVar(value=self.filter_obj.column if self.filter_obj and self.filter_obj.column else self.available_columns[0])
        self.column_combo = ttk.Combobox(col_frame, textvariable=self.column_var,
                                         values=self.available_columns, state='readonly', width=30)
        self.column_combo.grid(row=0, column=1, padx=5)
        self.column_combo.bind('<<ComboboxSelected>>', self.on_column_changed)

        # Filter frame (will be populated based on column type)
        self.filter_frame = ttk.LabelFrame(self.dialog, text="Filter Criteria", padding="10")
        self.filter_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=10, pady=5)

        # Initialize filter controls
        self.on_column_changed(None)

        # Buttons
        button_frame = ttk.Frame(self.dialog)
        button_frame.grid(row=2, column=0, pady=10)

        ttk.Button(button_frame, text="OK", command=self.ok_clicked).grid(row=0, column=0, padx=5)
        ttk.Button(button_frame, text="Cancel", command=self.cancel_clicked).grid(row=0, column=1, padx=5)

        # Configure grid
        self.dialog.columnconfigure(0, weight=1)
        self.dialog.rowconfigure(1, weight=1)

    def on_column_changed(self, event):
        """Update filter controls when column changes"""
        # Clear existing controls
        for widget in self.filter_frame.winfo_children():
            widget.destroy()

        column = self.column_var.get()
        if not column:
            return

        col_type = self.column_types[column]
        self.filter_frame.config(text=f"Filter Criteria for {column} [{col_type}]")

        if col_type == self.ColumnType.TEXT:
            self.create_text_filter()
        elif col_type == self.ColumnType.NUMBER:
            self.create_number_filter()
        elif col_type == self.ColumnType.DATE:
            self.create_date_filter()

    def create_text_filter(self):
        """Create text filter controls"""
        column = self.column_var.get()

        # Filter type
        self.text_type_var = tk.StringVar(value='equals')
        ttk.Radiobutton(self.filter_frame, text="Equals (select from list)",
                        variable=self.text_type_var, value='equals').grid(row=0, column=0, sticky=tk.W)
        ttk.Radiobutton(self.filter_frame, text="Contains",
                        variable=self.text_type_var, value='contains').grid(row=1, column=0, sticky=tk.W)

        # Get unique values
        unique_values = sorted(set(str(row.get(column, '')) for row in self.data
                                   if row.get(column) is not None))[:100]  # Limit to 100

        # Equals: Checkboxes in scrollable frame
        equals_frame = ttk.LabelFrame(self.filter_frame, text=f"Select values ({len(unique_values)} unique)", padding="5")
        equals_frame.grid(row=2, column=0, sticky=(tk.W, tk.E), pady=5)

        # Select all/none buttons
        button_frame = ttk.Frame(equals_frame)
        button_frame.grid(row=0, column=0, sticky=(tk.W, tk.E))

        self.check_vars = {}

        def select_all():
            for var in self.check_vars.values():
                var.set(True)

        def select_none():
            for var in self.check_vars.values():
                var.set(False)

        ttk.Button(button_frame, text="All", command=select_all, width=6).grid(row=0, column=0, padx=2)
        ttk.Button(button_frame, text="None", command=select_none, width=6).grid(row=0, column=1, padx=2)

        # Create canvas for scrolling
        canvas = tk.Canvas(equals_frame, height=150)
        scrollbar = ttk.Scrollbar(equals_frame, orient="vertical", command=canvas.yview)
        checkbox_frame = ttk.Frame(canvas)

        checkbox_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=checkbox_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.grid(row=1, column=0, sticky=(tk.W, tk.E))
        scrollbar.grid(row=1, column=1, sticky=(tk.N, tk.S))

        # Create checkboxes
        for i, value in enumerate(unique_values):
            var = tk.BooleanVar()
            self.check_vars[value] = var
            display_text = value if len(value) <= 40 else value[:37] + "..."
            cb = ttk.Checkbutton(checkbox_frame, text=display_text, variable=var)
            cb.grid(row=i, column=0, sticky=tk.W, padx=5)

        # Contains: Entry field
        ttk.Label(self.filter_frame, text="Contains text:").grid(row=3, column=0, sticky=tk.W, pady=(10, 0))
        self.contains_entry = ttk.Entry(self.filter_frame, width=40)
        self.contains_entry.grid(row=4, column=0, sticky=(tk.W, tk.E), pady=5)

        # Load existing values if editing
        if self.filter_obj and self.filter_obj.column == column and self.filter_obj.filter_config:
            filter_type = self.filter_obj.filter_config.get('type', 'equals')
            self.text_type_var.set(filter_type)

            if filter_type == 'equals' and 'values' in self.filter_obj.filter_config:
                for value in self.filter_obj.filter_config['values']:
                    if value in self.check_vars:
                        self.check_vars[value].set(True)
            elif filter_type == 'contains' and 'pattern' in self.filter_obj.filter_config:
                self.contains_entry.insert(0, self.filter_obj.filter_config['pattern'])

    def create_number_filter(self):
        """Create number filter controls"""
        ttk.Label(self.filter_frame, text="Minimum value:").grid(row=0, column=0, sticky=tk.W)
        self.min_entry = ttk.Entry(self.filter_frame, width=20)
        self.min_entry.grid(row=0, column=1, padx=5, pady=5)

        ttk.Label(self.filter_frame, text="Maximum value:").grid(row=1, column=0, sticky=tk.W)
        self.max_entry = ttk.Entry(self.filter_frame, width=20)
        self.max_entry.grid(row=1, column=1, padx=5, pady=5)

        ttk.Label(self.filter_frame, text="Leave empty for no limit",
                  font=('TkDefaultFont', 9, 'italic')).grid(row=2, column=0, columnspan=2, pady=5)

        # Load existing values if editing
        column = self.column_var.get()
        if self.filter_obj and self.filter_obj.column == column and self.filter_obj.filter_config:
            if self.filter_obj.filter_config.get('min') is not None:
                self.min_entry.insert(0, str(self.filter_obj.filter_config['min']))
            if self.filter_obj.filter_config.get('max') is not None:
                self.max_entry.insert(0, str(self.filter_obj.filter_config['max']))

    def create_date_filter(self):
        """Create date filter controls"""
        ttk.Label(self.filter_frame, text="From date (DD-MM-YYYY):").grid(row=0, column=0, sticky=tk.W)
        self.from_entry = ttk.Entry(self.filter_frame, width=20)
        self.from_entry.grid(row=0, column=1, padx=5, pady=5)

        ttk.Label(self.filter_frame, text="To date (DD-MM-YYYY):").grid(row=1, column=0, sticky=tk.W)
        self.to_entry = ttk.Entry(self.filter_frame, width=20)
        self.to_entry.grid(row=1, column=1, padx=5, pady=5)

        ttk.Label(self.filter_frame, text="Leave empty for no limit",
                  font=('TkDefaultFont', 9, 'italic')).grid(row=2, column=0, columnspan=2, pady=5)

        # Load existing values if editing
        column = self.column_var.get()
        if self.filter_obj and self.filter_obj.column == column and self.filter_obj.filter_config:
            if self.filter_obj.filter_config.get('from'):
                self.from_entry.insert(0, self.filter_obj.filter_config['from'].strftime('%d-%m-%Y'))
            if self.filter_obj.filter_config.get('to'):
                self.to_entry.insert(0, self.filter_obj.filter_config['to'].strftime('%d-%m-%Y'))

    def ok_clicked(self):
        """Validate and save the filter"""
        from main import DimensionalFilter  # Import from main module

        column = self.column_var.get()
        col_type = self.column_types[column]

        filter_obj = DimensionalFilter(column, col_type)

        if col_type == self.ColumnType.TEXT:
            filter_type = self.text_type_var.get()
            filter_obj.filter_config['type'] = filter_type

            if filter_type == 'equals':
                # Get selected values
                selected_values = [value for value, var in self.check_vars.items() if var.get()]
                if selected_values:
                    filter_obj.filter_config['values'] = selected_values
                else:
                    messagebox.showwarning("Warning", "No values selected. Filter will match nothing.")
                    return
            else:  # contains
                pattern = self.contains_entry.get().strip()
                if pattern:
                    filter_obj.filter_config['pattern'] = pattern
                else:
                    messagebox.showerror("Error", "Please enter a search pattern")
                    return

        elif col_type == self.ColumnType.NUMBER:
            try:
                min_val = self.min_entry.get().strip()
                max_val = self.max_entry.get().strip()

                if not min_val and not max_val:
                    messagebox.showerror("Error", "Please enter at least one value")
                    return

                if min_val:
                    filter_obj.filter_config['min'] = float(min_val.replace(',', '.'))
                if max_val:
                    filter_obj.filter_config['max'] = float(max_val.replace(',', '.'))

                if min_val and max_val and filter_obj.filter_config['min'] > filter_obj.filter_config['max']:
                    messagebox.showerror("Error", "Minimum value must be less than maximum value")
                    return
            except ValueError:
                messagebox.showerror("Error", "Invalid number format")
                return

        elif col_type == self.ColumnType.DATE:
            try:
                from_date = self.from_entry.get().strip()
                to_date = self.to_entry.get().strip()

                if not from_date and not to_date:
                    messagebox.showerror("Error", "Please enter at least one date")
                    return

                if from_date:
                    filter_obj.filter_config['from'] = datetime.strptime(from_date, '%d-%m-%Y')
                if to_date:
                    filter_obj.filter_config['to'] = datetime.strptime(to_date, '%d-%m-%Y')

                if from_date and to_date and filter_obj.filter_config['from'] > filter_obj.filter_config['to']:
                    messagebox.showerror("Error", "From date must be before to date")
                    return
            except ValueError:
                messagebox.showerror("Error", "Invalid date format. Use DD-MM-YYYY")
                return

        self.result = filter_obj
        self.dialog.destroy()

    def cancel_clicked(self):
        self.dialog.destroy()


class SamplingRuleDialog:
    """Dialog for creating/editing a sampling rule with quota"""
    def __init__(self, parent, title, column_names, column_types, data, ColumnType, rule=None):
        self.result = None
        self.column_names = column_names
        self.column_types = column_types
        self.data = data
        self.ColumnType = ColumnType

        # Create dialog
        self.dialog = tk.Toplevel(parent)
        self.dialog.title(title)
        self.dialog.geometry("500x550")
        self.dialog.transient(parent)
        self.dialog.grab_set()

        # Initialize with existing rule or create new
        self.rule = rule

        self.create_widgets()

        # Center the dialog
        self.dialog.update_idletasks()
        x = (self.dialog.winfo_screenwidth() // 2) - (self.dialog.winfo_width() // 2)
        y = (self.dialog.winfo_screenheight() // 2) - (self.dialog.winfo_height() // 2)
        self.dialog.geometry(f"+{x}+{y}")

    def create_widgets(self):
        # Rule name
        name_frame = ttk.Frame(self.dialog, padding="10")
        name_frame.grid(row=0, column=0, sticky=(tk.W, tk.E))

        ttk.Label(name_frame, text="Rule Name:").grid(row=0, column=0, sticky=tk.W)
        self.name_entry = ttk.Entry(name_frame, width=30)
        self.name_entry.grid(row=0, column=1, padx=5)
        if self.rule:
            self.name_entry.insert(0, self.rule.name)

        # Sample count
        ttk.Label(name_frame, text="Number of Samples:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.sample_spinbox = ttk.Spinbox(name_frame, from_=1, to=10000, width=10)
        self.sample_spinbox.grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)
        self.sample_spinbox.set(self.rule.sample_count if self.rule else 5)

        # Column selection
        col_frame = ttk.Frame(self.dialog, padding="10")
        col_frame.grid(row=1, column=0, sticky=(tk.W, tk.E))

        ttk.Label(col_frame, text="Select Column:").grid(row=0, column=0, sticky=tk.W)
        self.column_var = tk.StringVar(value=self.rule.column if self.rule and self.rule.column else self.column_names[0])
        self.column_combo = ttk.Combobox(col_frame, textvariable=self.column_var,
                                         values=self.column_names, state='readonly', width=30)
        self.column_combo.grid(row=0, column=1, padx=5)
        self.column_combo.bind('<<ComboboxSelected>>', self.on_column_changed)

        # Filter frame (will be populated based on column type)
        self.filter_frame = ttk.LabelFrame(self.dialog, text="Sampling Criteria", padding="10")
        self.filter_frame.grid(row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=10, pady=5)

        # Initialize filter controls
        self.on_column_changed(None)

        # Buttons
        button_frame = ttk.Frame(self.dialog)
        button_frame.grid(row=3, column=0, pady=10)

        ttk.Button(button_frame, text="OK", command=self.ok_clicked).grid(row=0, column=0, padx=5)
        ttk.Button(button_frame, text="Cancel", command=self.cancel_clicked).grid(row=0, column=1, padx=5)

        # Configure grid
        self.dialog.columnconfigure(0, weight=1)
        self.dialog.rowconfigure(2, weight=1)

    def on_column_changed(self, event):
        """Update filter controls when column changes"""
        # Clear existing controls
        for widget in self.filter_frame.winfo_children():
            widget.destroy()

        column = self.column_var.get()
        if not column:
            return

        col_type = self.column_types[column]
        self.filter_frame.config(text=f"Sampling Criteria for {column} [{col_type}]")

        if col_type == self.ColumnType.TEXT:
            self.create_text_filter()
        elif col_type == self.ColumnType.NUMBER:
            self.create_number_filter()
        elif col_type == self.ColumnType.DATE:
            self.create_date_filter()

    def create_text_filter(self):
        """Create text filter controls - simplified for single values"""
        column = self.column_var.get()

        # Get unique values
        unique_values = sorted(set(str(row.get(column, '')) for row in self.data
                                   if row.get(column) is not None))[:100]  # Limit to 100

        # Single selection for sampling rules
        ttk.Label(self.filter_frame, text="Select value(s) to sample:").grid(row=0, column=0, sticky=tk.W)

        # Checkboxes in scrollable frame
        checkbox_frame_container = ttk.Frame(self.filter_frame)
        checkbox_frame_container.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=5)

        # Create canvas for scrolling
        canvas = tk.Canvas(checkbox_frame_container, height=200)
        scrollbar = ttk.Scrollbar(checkbox_frame_container, orient="vertical", command=canvas.yview)
        checkbox_frame = ttk.Frame(canvas)

        checkbox_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=checkbox_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.grid(row=0, column=0, sticky=(tk.W, tk.E))
        scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))

        # Create checkboxes
        self.check_vars = {}
        for i, value in enumerate(unique_values):
            var = tk.BooleanVar()
            self.check_vars[value] = var
            display_text = value if len(value) <= 40 else value[:37] + "..."
            cb = ttk.Checkbutton(checkbox_frame, text=display_text, variable=var)
            cb.grid(row=i, column=0, sticky=tk.W, padx=5)

        # Load existing values if editing
        if self.rule and self.rule.column == column and self.rule.filter_config:
            if 'values' in self.rule.filter_config:
                for value in self.rule.filter_config['values']:
                    if value in self.check_vars:
                        self.check_vars[value].set(True)

    def create_number_filter(self):
        """Create number filter controls"""
        ttk.Label(self.filter_frame, text="Sample from range:").grid(row=0, column=0, sticky=tk.W, pady=5)

        ttk.Label(self.filter_frame, text="Minimum value:").grid(row=1, column=0, sticky=tk.W)
        self.min_entry = ttk.Entry(self.filter_frame, width=20)
        self.min_entry.grid(row=1, column=1, padx=5, pady=5)

        ttk.Label(self.filter_frame, text="Maximum value:").grid(row=2, column=0, sticky=tk.W)
        self.max_entry = ttk.Entry(self.filter_frame, width=20)
        self.max_entry.grid(row=2, column=1, padx=5, pady=5)

        ttk.Label(self.filter_frame, text="Leave empty for no limit",
                  font=('TkDefaultFont', 9, 'italic')).grid(row=3, column=0, columnspan=2, pady=5)

        # Load existing values if editing
        column = self.column_var.get()
        if self.rule and self.rule.column == column and self.rule.filter_config:
            if self.rule.filter_config.get('min') is not None:
                self.min_entry.insert(0, str(self.rule.filter_config['min']))
            if self.rule.filter_config.get('max') is not None:
                self.max_entry.insert(0, str(self.rule.filter_config['max']))

    def create_date_filter(self):
        """Create date filter controls"""
        ttk.Label(self.filter_frame, text="Sample from date range:").grid(row=0, column=0, sticky=tk.W, pady=5)

        ttk.Label(self.filter_frame, text="From date (DD-MM-YYYY):").grid(row=1, column=0, sticky=tk.W)
        self.from_entry = ttk.Entry(self.filter_frame, width=20)
        self.from_entry.grid(row=1, column=1, padx=5, pady=5)

        ttk.Label(self.filter_frame, text="To date (DD-MM-YYYY):").grid(row=2, column=0, sticky=tk.W)
        self.to_entry = ttk.Entry(self.filter_frame, width=20)
        self.to_entry.grid(row=2, column=1, padx=5, pady=5)

        ttk.Label(self.filter_frame, text="Leave empty for no limit",
                  font=('TkDefaultFont', 9, 'italic')).grid(row=3, column=0, columnspan=2, pady=5)

        # Load existing values if editing
        column = self.column_var.get()
        if self.rule and self.rule.column == column and self.rule.filter_config:
            if self.rule.filter_config.get('from'):
                self.from_entry.insert(0, self.rule.filter_config['from'].strftime('%d-%m-%Y'))
            if self.rule.filter_config.get('to'):
                self.to_entry.insert(0, self.rule.filter_config['to'].strftime('%d-%m-%Y'))

    def ok_clicked(self):
        """Validate and save the rule"""
        from main import SamplingRule  # Import from main module

        # Validate name
        name = self.name_entry.get().strip()
        if not name:
            messagebox.showerror("Error", "Please enter a rule name")
            return

        # Validate sample count
        try:
            sample_count = int(self.sample_spinbox.get())
        except ValueError:
            messagebox.showerror("Error", "Invalid sample count")
            return

        column = self.column_var.get()
        col_type = self.column_types[column]

        rule = SamplingRule(name, column, col_type)
        rule.sample_count = sample_count

        # Set filter config based on type
        if col_type == self.ColumnType.TEXT:
            # Get selected values
            selected_values = [value for value, var in self.check_vars.items() if var.get()]
            if selected_values:
                rule.filter_config['type'] = 'equals'
                rule.filter_config['values'] = selected_values
            else:
                messagebox.showerror("Error", "Please select at least one value")
                return

        elif col_type == self.ColumnType.NUMBER:
            try:
                min_val = self.min_entry.get().strip()
                max_val = self.max_entry.get().strip()

                if not min_val and not max_val:
                    messagebox.showerror("Error", "Please enter at least one value")
                    return

                if min_val:
                    rule.filter_config['min'] = float(min_val.replace(',', '.'))
                if max_val:
                    rule.filter_config['max'] = float(max_val.replace(',', '.'))

                if min_val and max_val and rule.filter_config['min'] > rule.filter_config['max']:
                    messagebox.showerror("Error", "Minimum value must be less than maximum value")
                    return
            except ValueError:
                messagebox.showerror("Error", "Invalid number format")
                return

        elif col_type == self.ColumnType.DATE:
            try:
                from_date = self.from_entry.get().strip()
                to_date = self.to_entry.get().strip()

                if not from_date and not to_date:
                    messagebox.showerror("Error", "Please enter at least one date")
                    return

                if from_date:
                    rule.filter_config['from'] = datetime.strptime(from_date, '%d-%m-%Y')
                if to_date:
                    rule.filter_config['to'] = datetime.strptime(to_date, '%d-%m-%Y')

                if from_date and to_date and rule.filter_config['from'] > rule.filter_config['to']:
                    messagebox.showerror("Error", "From date must be before to date")
                    return
            except ValueError:
                messagebox.showerror("Error", "Invalid date format. Use DD-MM-YYYY")
                return

        self.result = rule
        self.dialog.destroy()

    def cancel_clicked(self):
        self.dialog.destroy()
