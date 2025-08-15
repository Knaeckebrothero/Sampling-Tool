# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a German financial audit tool ("Finanzdaten Stichprobentest") that performs stratified sampling on financial datasets. The application provides both desktop (Tkinter) and web (Streamlit) interfaces for financial data sampling and analysis.

The project now supports both SQLite (default) and MS SQL Server databases, with Docker support for development environments.

## Configuration

The application uses environment variables for configuration. Create a `.env` file in the repository root:

```bash
# SQLite configuration (default)
DB_PATH=./sampling.db

# MS SQL Server configuration (optional)
MSSQL_SERVER=localhost
MSSQL_DATABASE=SamplingDB
MSSQL_USERNAME=sa
MSSQL_PASSWORD=YourStrong@Passw0rd

# Download path for exports
COPY_PATH=C:\Users\YourUser\Downloads
```

## Key Commands

### Installation and Dependencies
```bash
# Install core dependencies
pip install -r requirements.txt

# Install full dependencies (includes Streamlit for web UI)
pip install -r "documentation/Installation & Start/requirements.txt"
```

### Running the Application
```bash
# Main desktop application
python src/main.py

# Web application (Streamlit)
streamlit run src/streamlit_app.py

# Alternative entry points
python sample_testing_standard.py    # Desktop version
python sample_testing_advanced.py    # Web version
streamlit run sample_testing_advanced.py

# Windows batch scripts (in documentation/Installation & Start/)
install.bat           # One-time setup with virtual environment
start_standard.bat    # Start desktop version
start_erweitert.bat   # Start web version
```

### Database Operations
```bash
# Initialize SQLite database with sample data
python db_init.py

# Initialize MS SQL Server database
python src/init_mssql_db.py

# Test database connections
python test_connection.py

# Docker MS SQL Server (for development)
docker-compose up -d
```

## Architecture Overview

### Core Components

- **`src/main.py`**: Main application with core sampling logic and DataHandler class
- **`src/database_mssql.py`**: Database abstraction layer supporting both SQLite and MS SQL Server
- **`src/ui_tkinter.py`**: Desktop GUI implementation using Tkinter
- **`src/streamlit_app.py`**: Web GUI implementation using Streamlit
- **`src/init_mssql_db.py`**: MS SQL Server initialization script
- **`src/schema.sql`**: Database schema definition

### Database Support

The project uses a database abstraction layer (`src/database_mssql.py`) that supports:
- **SQLite**: Default, lightweight, file-based database
- **MS SQL Server**: Enterprise database with Windows Authentication and SQL Authentication support
- **Docker MS SQL Server**: Development environment using `docker-compose.yml`

### Key Classes

- **`DataHandler`** (`src/main.py`): Main business logic controller
  - `load_data()`: Loads data from selected table
  - `apply_global_filters()`: Applies dimensional filters using SQL
  - `generate_stratified_sample()`: Executes stratified sampling
  - `save_configuration()` / `load_configuration()`: Persist filter configurations

- **`Database`** (`src/database_mssql.py`): Database abstraction layer
  - Singleton pattern with `get_instance()`
  - Supports SQLite and MS SQL Server
  - Connection pooling and parameterized queries

- **`DimensionalFilter`** (`src/main.py`): Global filtering logic
  - Supports TEXT, NUMBER, and DATE column types
  - `to_sql_where()`: Generates SQL WHERE clauses dynamically

- **`SamplingRule`** (`src/main.py`): Stratified sampling rules
  - Configurable by dimension, value, sample size, and type
  - `matches()`: Tests if a row matches the rule criteria

## Database Schema

Three main tables:
- **`kundenstamm`**: Customer master data (49 columns) - main entity table
- **`softfact_vw`**: Software facts view (14 columns) - transaction data
- **`kontodaten_vw`**: Account data view (14 columns) - account information

Key relationships:
- JOIN on `personennummer_pseudonym` and `banknummer`
- Tables support German special characters (ä, ö, ü, ß)
- Column names are automatically cleaned (spaces → underscores)

## Testing and Quality

### Testing Database Connections
```bash
# Run comprehensive connection tests
python test_connection.py
```

This will test:
- SQLite connection
- MS SQL Server (Docker)
- MS SQL Server (Windows Authentication)
- Available ODBC drivers

### Currently Missing (Future Development)
- No automated unit tests
- No linting configuration (consider pylint, black, mypy)
- No CI/CD pipeline

## Development Workflow

### Adding New Sampling Rules
1. Modify `SamplingRule` class in `src/main.py`
2. Update UI components in `src/ui_tkinter.py` or `src/streamlit_app.py`
3. Test with sample data using `python db_init.py`

### Database Schema Changes
1. Update `src/schema.sql`
2. Modify `Database` class methods in `src/database_mssql.py`
3. Update initialization scripts (`db_init.py` or `src/init_mssql_db.py`)
4. Reinitialize database

### Switching Between Databases
```python
# In code, use the Database factory
from src.database_mssql import Database

# SQLite (default)
db = Database.get_instance(db_type='sqlite')

# MS SQL Server
db = Database.get_instance(db_type='mssql', connection_params={...})
```

## Docker Development Environment

Start MS SQL Server in Docker:
```bash
# Start container
docker-compose up -d

# Initialize database
python src/init_mssql_db.py

# Stop container
docker-compose down
```

## Language and Localization

The application is designed for German-speaking users:
- UI labels and messages are in German
- Date formats: DD.MM.YYYY
- Number formats: comma as decimal separator (1.234,56)
- All user-facing text in German

## Important Implementation Details

### European Number Format Handling
Automatic detection and conversion:
- Comma (,) as decimal separator
- Period (.) as thousands separator
- Handled in `detect_column_type()` method

### Date Format Detection
Supports multiple formats:
- DD.MM.YYYY (primary)
- YYYY-MM-DD
- DD/MM/YYYY
- Auto-detection in `detect_column_type()`

### SQL Injection Prevention
All database queries use parameterized statements:
```python
query = f"SELECT * FROM {table_name} WHERE {where_clause}"
cursor.execute(query, params)  # params are sanitized
```

### Performance Considerations
- Database singleton pattern prevents connection overhead
- Batch operations for large datasets
- Indexed columns for filtering operations
- Connection pooling for MS SQL Server

## Security Considerations

- Parameterized queries prevent SQL injection
- Environment variables for sensitive configuration
- No hardcoded credentials in code
- Local deployment only (no network exposure by default)
- File path validation during import operations