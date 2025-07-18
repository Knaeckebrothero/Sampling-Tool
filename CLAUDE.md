# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a German financial audit tool ("Finanzdaten Stichprobentest") that performs stratified sampling on financial datasets. The application provides both desktop (Tkinter) and web (Streamlit) interfaces for financial data sampling and analysis.

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
# Initialize database
python src/db_init.py

# The database file is located at:
# - src/data/sampling.db (main location)
# - data/sampling.db (alternative location)
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

### Key Classes and Modules

- **`Database`**: Manages SQLite connections and operations (`src/database.py`)
- **`DimensionalFilter`**: Handles global filtering logic (`src/main.py`)
- **`SamplingRule`**: Manages stratified sampling rules (`src/main.py`)
- **`SimpleSampleTestingApp`**: Main Tkinter GUI application (`src/ui_tkinter.py`)

## Database Schema

The application uses SQLite with a dynamic schema that adapts to imported CSV data:
- Main table: `financial_data` (structure depends on imported data)
- Typical columns: `id`, `key_figure`, `value`, `date`, `legal_form`
- Column names are automatically cleaned (spaces → underscores)

## Dependencies

### Core Dependencies
- `pandas`: Data manipulation and analysis
- `python-dotenv`: Environment variable management
- `tkinter`: Desktop GUI (part of Python standard library)

### Extended Dependencies (for web version)
- `streamlit`: Web application framework
- `openpyxl`: Excel file support

## Testing and Quality

**Note**: This project currently lacks automated testing infrastructure:
- No test files exist (only placeholder `tests/` directory)
- No linting configuration
- No type checking setup
- No CI/CD pipeline beyond basic placeholder

When adding tests, consider:
- Testing data import functionality
- Validating sampling algorithms
- Testing filter logic
- GUI component testing

## Development Workflow

### Current Branch Structure
- Main branch: `main`
- Current development: `feature/separate-database`

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
- Error messages and logs may be in German
- Date formats follow German conventions

## Common Operations

### Adding New Sampling Rules
1. Modify the `SamplingRule` class in `src/main.py`
2. Update the UI components in `src/ui_tkinter.py`
3. Test with sample data

### Database Schema Changes
1. Update `src/schema.sql`
2. Modify `src/database.py` for new operations
3. Update `src/db_init.py` for initialization

### Adding New Filter Types
1. Extend `DimensionalFilter` class in `src/main.py`
2. Update filter UI in the appropriate interface files
3. Test with various data types

## Security Considerations

- Database operations use parameterized queries to prevent SQL injection
- File paths are validated during import operations
- The application runs locally with no network exposure by default