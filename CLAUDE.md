# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a German financial audit tool ("Finanzdaten Stichprobentest") that performs stratified sampling on financial datasets. The application provides both desktop (Tkinter) and web (Streamlit) interfaces for financial data sampling and analysis.

## Configuration

The application uses environment variables for configuration. Create a `.env` file in the repository root:

```bash
# Database path (default: ./sampling.db)
DB_PATH=./sampling.db

# Download path for exports
COPY_PATH=C:\Users\YourUser\Downloads
```

## Key Commands

### Installation and Dependencies
```bash
# Install dependencies for basic functionality
pip install -r requirements.txt

# Install dependencies for full functionality (including web UI)
pip install -r "Installation & Start/requirements.txt"
```

### Running the Application
```bash
# Main application (desktop interface)
python src/main.py

# Alternative versions:
python sample_testing_standard.py    # Desktop version
python sample_testing_advanced.py    # Web version (requires streamlit)
streamlit run sample_testing_advanced.py  # Web version via Streamlit

# Windows batch scripts (in Installation & Start/)
install.bat           # One-time setup
start_standard.bat    # Start desktop version
start_erweitert.bat   # Start web version
```

### Database Operations
```bash
# Initialize database with sample data (simplified)
python db_init.py

# The database will be created at:
# ./sampling.db (in the repository root)

# Note: db_init.py has been moved to repository root and automatically:
# - Loads schema from src/schema.sql
# - Imports sample data from sample_data/ directory
# - Creates all three tables: kundenstamm, softfact_vw, kontodaten_vw
```

## Architecture Overview

### Core Components

- **`src/main.py`**: Main application entry point with core sampling logic
- **`src/ui_tkinter.py`**: Desktop GUI implementation using Tkinter
- **`src/database.py`**: SQLite database operations and connection management
- **`src/import_csv.py`**: CSV/Excel data import functionality
- **`src/db_init.py`**: Database initialization and schema setup
- **`src/schema.sql`**: Database schema definition

### Application Variants

The project contains multiple application variants:
- **Standard**: Desktop-only version using Tkinter (`sample_testing_standard.py`)
- **Advanced**: Web-based version using Streamlit (`sample_testing_advanced.py`)
- **Combined**: Feature-rich version with both interfaces (`sample_testing_combined.py`)

### Data Flow

1. **Data Import**: CSV/Excel files are imported via pandas and stored in SQLite
2. **Filtering**: Global dimensional filters are applied to the dataset
3. **Sampling**: Stratified sampling is performed based on configurable rules
4. **Export**: Results can be exported in various formats

### Key Classes and Methods

- **`DataHandler`**: Main business logic controller (`src/main.py`)
  - `load_data()`: Loads data from selected table
  - `apply_filters()`: Applies dimensional filters
  - `perform_sampling()`: Executes stratified sampling

- **`Database`**: SQLite connection management (`src/database.py`)
  - `get_db()`: Factory method for database connections
  - `execute_query()`: Safe query execution with parameters

- **`DimensionalFilter`**: Global filtering logic (`src/main.py`)
  - Supports TEXT, NUMBER, and DATE column types
  - Generates SQL WHERE clauses dynamically

- **`SamplingRule`**: Stratified sampling rules (`src/main.py`)
  - Configurable by dimension, value, sample size, and type

## Database Schema

The application uses three main tables:
- **`kundenstamm`**: Customer master data (49 columns)
- **`softfact_vw`**: Software facts view (14 columns)
- **`kontodaten_vw`**: Account data view (14 columns)

Column names are automatically cleaned (spaces → underscores) and support German special characters.

## Dependencies

### Core Dependencies
- `pandas`: Data manipulation and analysis
- `python-dotenv`: Environment variable management
- `tkinter`: Desktop GUI (part of Python standard library)

### Extended Dependencies (for web version)
- `streamlit`: Web application framework
- `openpyxl`: Excel file support

## Testing and Quality

**Note**: This project currently lacks automated testing infrastructure. When adding tests, consider:
- Testing data import functionality with various CSV/Excel formats
- Validating sampling algorithms with known datasets
- Testing filter logic with edge cases
- GUI component testing for both interfaces

### Linting and Type Checking
No linting or type checking is currently configured. Future setup should include:
```bash
# Suggested commands (not yet implemented)
# pylint src/
# mypy src/
# black src/
```

## Development Workflow

### File Organization
- Source code: `src/`
- Documentation: `Dokumentation/` (German)
- Installation scripts: `Installation & Start/`
- Example data: `example_data.csv`, `example_data.xlsx`
- Additional scripts: `Python-Skripte/`

## Language and Localization

The application is designed for German-speaking users:
- UI labels and messages are in German
- Documentation is in German
- Date formats follow German conventions (DD.MM.YYYY)
- Number formats support comma as decimal separator

## Common Operations

### Adding New Sampling Rules
1. Modify the `SamplingRule` class in `src/main.py`
2. Update the UI components in `src/ui_tkinter.py` or Streamlit interface
3. Test with sample data

### Database Schema Changes
1. Update `src/schema.sql`
2. Modify `src/database.py` for new operations
3. Update `src/db_init.py` for initialization
4. Run `python src/db_init.py` to reinitialize

### Adding New Filter Types
1. Extend `ColumnType` enum in `src/main.py`
2. Add new filter logic in `DimensionalFilter` class
3. Update UI components to support new filter type

## Important Implementation Details

### European Number Format Handling
The application automatically detects and converts European number formats:
- Comma (,) as decimal separator
- Period (.) as thousands separator

### Date Format Detection
Supports multiple date formats including:
- DD.MM.YYYY
- YYYY-MM-DD
- DD/MM/YYYY

### SQL Query Generation
All database queries use parameterized statements to prevent SQL injection:
```python
# Example from codebase
query = f"SELECT * FROM {table_name} WHERE {where_clause}"
cursor.execute(query, params)
```

## Security Considerations

- Database operations use parameterized queries to prevent SQL injection
- File paths are validated during import operations
- The application runs locally with no network exposure by default
- No authentication system is implemented (local use assumed)