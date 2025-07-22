import streamlit as st
import pandas as pd
from datetime import datetime, date
import os
import sys
from dotenv import load_dotenv

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Import database module
from database import Database

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Finanzdaten Stichprobentest",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize database connection
@st.cache_resource
def get_database():
    """Get database connection (cached)"""
    return Database.get_instance()

# Function to format dates in German format
def format_date_german(date_obj):
    """Format date in German format DD.MM.YYYY"""
    if pd.isna(date_obj):
        return ""
    if isinstance(date_obj, str):
        return date_obj
    return date_obj.strftime('%d.%m.%Y')

# Function to format numbers in European format
def format_number_european(value):
    """Format number with comma as decimal separator"""
    if pd.isna(value):
        return ""
    try:
        return f"{float(value):,.2f}".replace(',', ' ').replace('.', ',')
    except:
        return str(value)

# Main app
def main():
    # Initialize session state
    if 'current_page' not in st.session_state:
        st.session_state.current_page = 'home'
    
    # Sidebar navigation
    with st.sidebar:
        st.title("🏦 Navigation")
        
        # Navigation buttons
        if st.button("🏠 Startseite", use_container_width=True):
            st.session_state.current_page = 'home'
        
        if st.button("👤 Natürliche Personen", use_container_width=True):
            st.session_state.current_page = 'natural_persons'
            
        if st.button("🏢 Juristische Personen", use_container_width=True):
            st.session_state.current_page = 'legal_entities'
            
        if st.button("ℹ️ Über", use_container_width=True):
            st.session_state.current_page = 'about'
    
    # Display current page
    if st.session_state.current_page == 'home':
        show_home_page()
    elif st.session_state.current_page == 'natural_persons':
        show_natural_persons_page()
    elif st.session_state.current_page == 'legal_entities':
        show_legal_entities_page()
    elif st.session_state.current_page == 'about':
        show_about_page()

def show_home_page():
    """Display the home/landing page"""
    st.title("🏦 Finanzdaten Stichprobentest")
    st.markdown("---")
    
    st.markdown("""
    ## Willkommen zum Finanzdaten Stichprobentest Tool
    
    Dieses Tool ermöglicht die Durchführung von Stichprobenziehungen aus Finanzdaten für Prüfungszwecke.
    
    ### 📋 Funktionen:
    
    **1. Natürliche Personen** 👤
    - Ziehen Sie eine zufällige Stichprobe von natürlichen Personen
    - Filtern Sie nach Zeitraum
    - Exportieren Sie die Ergebnisse
    
    **2. Juristische Personen** 🏢
    - Ziehen Sie geschichtete Stichproben nach Rechtsform
    - Definieren Sie unterschiedliche Stichprobengrößen für verschiedene Unternehmenstypen
    - Filtern Sie nach Zeitraum
    
    **3. Über** ℹ️
    - Übersicht über die verfügbaren Datenbanktabellen
    - Technische Informationen
    
    ### 🚀 Erste Schritte:
    
    1. Wählen Sie im linken Menü die gewünschte Funktion
    2. Geben Sie Ihre Filterkriterien ein
    3. Klicken Sie auf "Filter anwenden"
    4. Die Ergebnisse werden als Tabelle angezeigt
    
    ### 📊 Datengrundlage:
    
    Die Stichproben werden aus drei Haupttabellen gezogen:
    - **kundenstamm**: Stammdaten der Kunden
    - **softfact_vw**: Softfacts-Informationen
    - **kontodaten_vw**: Kontoinformationen
    """)

def show_natural_persons_page():
    """Display the natural persons sampling page"""
    st.title("👤 Stichprobenziehung - Natürliche Personen")
    st.markdown("---")
    
    # Get database connection
    db = get_database()
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("📋 Stichprobenparameter")
        
        # Sample size input
        sample_size = st.number_input(
            "Anzahl der zu ziehenden Personen:",
            min_value=1,
            max_value=10000,
            value=10,
            step=1
        )
        
        # Show accounts checkbox
        include_accounts = st.checkbox(
            "🔗 Mit Kontoinformationen verknüpfen (JOIN mit kontodaten_vw)",
            value=False,
            help="Zeigt zusätzliche Informationen aus der Kontodaten-Tabelle an",
            key="natural_include_accounts"
        )
        
        # Date range inputs
        st.subheader("📅 Zeitraum")
        date_col1, date_col2 = st.columns(2)
        
        with date_col1:
            start_date = st.date_input(
                "Von:",
                value=date(2020, 1, 1),
                format="DD.MM.YYYY"
            )
        
        with date_col2:
            end_date = st.date_input(
                "Bis:",
                value=date.today(),
                format="DD.MM.YYYY"
            )
        
        # Apply button
        if st.button("🔍 Filter anwenden", type="primary", use_container_width=True):
            with st.spinner("Ziehe Stichprobe..."):
                try:
                    # Build SQL query for natural persons
                    if include_accounts:
                        # Query with JOIN to get account information
                        query = """
                        SELECT 
                            k.*,
                            COUNT(DISTINCT kd.kontonummer_pseudonym) as anzahl_konten,
                            SUM(CASE WHEN kd.treuhandkonto = 'J' THEN 1 ELSE 0 END) as anzahl_treuhandkonten,
                            SUM(CASE WHEN kd.anderkonto = 'J' THEN 1 ELSE 0 END) as anzahl_anderkonten,
                            SUM(CASE WHEN kd.konto_fuer_fremde_rechnung = 'J' THEN 1 ELSE 0 END) as anzahl_fremdkonten,
                            MIN(kd.kontoeroeffnung) as erstes_konto_datum,
                            MAX(kd.kontoeroeffnung) as letztes_konto_datum
                        FROM kundenstamm k
                        LEFT JOIN kontodaten_vw kd 
                            ON k.personennummer_pseudonym = kd.personennummer_pseudonym 
                            AND k.banknummer = kd.banknummer
                        WHERE k.rechtsformauspraegung_beschreibung_1 = 'Natürliche Person'
                        AND k.stichtag >= ?
                        AND k.stichtag <= ?
                        GROUP BY k.pk
                        ORDER BY RANDOM()
                        LIMIT ?
                        """
                    else:
                        # Simple query without JOIN
                        query = """
                        SELECT * FROM kundenstamm 
                        WHERE rechtsformauspraegung_beschreibung_1 = 'Natürliche Person'
                        AND stichtag >= ?
                        AND stichtag <= ?
                        ORDER BY RANDOM()
                        LIMIT ?
                        """
                    
                    params = (
                        start_date.strftime('%Y-%m-%d'),
                        end_date.strftime('%Y-%m-%d'),
                        sample_size
                    )
                    
                    # Execute query
                    result = db.cursor.execute(query, params).fetchall()
                    
                    if result:
                        # Convert to DataFrame
                        df = pd.DataFrame([dict(row) for row in result])
                        
                        # Store in session state
                        st.session_state.natural_persons_results = df
                        st.success(f"✅ {len(df)} Datensätze gefunden!")
                    else:
                        st.warning("Keine Datensätze gefunden.")
                        st.session_state.natural_persons_results = None
                        
                except Exception as e:
                    st.error(f"Fehler bei der Abfrage: {str(e)}")
                    st.session_state.natural_persons_results = None
    
    with col2:
        st.subheader("📊 Statistik")
        
        # Show total count of natural persons
        try:
            count_query = """
            SELECT COUNT(*) as total FROM kundenstamm 
            WHERE rechtsformauspraegung_beschreibung_1 = 'Natürliche Person'
            AND stichtag >= ?
            AND stichtag <= ?
            """
            
            count_result = db.cursor.execute(count_query, (
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d')
            )).fetchone()
            
            total_count = count_result['total'] if count_result else 0
            
            st.metric("Verfügbare Personen im Zeitraum", f"{total_count:,}".replace(',', '.'))
            st.metric("Gewünschte Stichprobengröße", sample_size)
            
            if total_count > 0:
                percentage = (sample_size / total_count) * 100
                st.metric("Stichprobenanteil", f"{percentage:.1f}%")
            
        except Exception as e:
            st.error(f"Fehler beim Abrufen der Statistik: {str(e)}")
    
    # Display results
    if hasattr(st.session_state, 'natural_persons_results') and st.session_state.natural_persons_results is not None:
        st.markdown("---")
        st.subheader("📋 Ergebnisse")
        
        df = st.session_state.natural_persons_results
        
        # Select columns to display
        display_columns = [
            'kundennummer', 'banknummer', 'stichtag',
            'geburtsdatum_gruendungsdatum_pseudonym',
            'postleitzahl_pseudonym', 'ort_pseudonym',
            'land_bezeichnung_pseudonym'
        ]
        
        # Add account columns if JOIN was used
        if include_accounts and 'anzahl_konten' in df.columns:
            display_columns.extend([
                'anzahl_konten', 'anzahl_treuhandkonten', 
                'anzahl_anderkonten', 'anzahl_fremdkonten',
                'erstes_konto_datum', 'letztes_konto_datum'
            ])
        
        # Filter available columns
        available_columns = [col for col in display_columns if col in df.columns]
        
        # Format dates
        if 'stichtag' in df.columns:
            df['stichtag'] = pd.to_datetime(df['stichtag'], dayfirst=True).dt.strftime('%d.%m.%Y')
        if 'erstes_konto_datum' in df.columns and 'erstes_konto_datum' in available_columns:
            df['erstes_konto_datum'] = pd.to_datetime(df['erstes_konto_datum'], dayfirst=True, errors='coerce').dt.strftime('%d.%m.%Y')
        if 'letztes_konto_datum' in df.columns and 'letztes_konto_datum' in available_columns:
            df['letztes_konto_datum'] = pd.to_datetime(df['letztes_konto_datum'], dayfirst=True, errors='coerce').dt.strftime('%d.%m.%Y')
        
        # Display dataframe
        st.dataframe(
            df[available_columns],
            use_container_width=True,
            hide_index=True
        )
        
        # Show account statistics if JOIN was used
        if include_accounts and 'anzahl_konten' in df.columns:
            st.subheader("🏦 Kontoinformationen (aus JOIN)")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_accounts = df['anzahl_konten'].sum()
                st.metric("Gesamtanzahl Konten", f"{total_accounts:,}".replace(',', '.'))
            
            with col2:
                trust_accounts = df['anzahl_treuhandkonten'].sum()
                st.metric("Treuhandkonten", f"{trust_accounts:,}".replace(',', '.'))
            
            with col3:
                other_accounts = df['anzahl_anderkonten'].sum()
                st.metric("Anderkonten", f"{other_accounts:,}".replace(',', '.'))
                
            with col4:
                foreign_accounts = df['anzahl_fremdkonten'].sum()
                st.metric("Fremdkonten", f"{foreign_accounts:,}".replace(',', '.'))
            
            # Show persons with multiple accounts
            multi_account_persons = df[df['anzahl_konten'] > 1]
            if len(multi_account_persons) > 0:
                st.write(f"**Personen mit mehreren Konten:** {len(multi_account_persons)} von {len(df)} ({len(multi_account_persons)/len(df)*100:.1f}%)")
        
        # Export button
        csv = df.to_csv(index=False, sep=';')
        st.download_button(
            label="📥 Als CSV exportieren",
            data=csv,
            file_name=f"stichprobe_natuerliche_personen_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

def show_legal_entities_page():
    """Display the legal entities sampling page"""
    st.title("🏢 Stichprobenziehung - Juristische Personen")
    st.markdown("---")
    
    # Get database connection
    db = get_database()
    
    # First, get available legal forms
    try:
        forms_query = """
        SELECT DISTINCT 
            rechtsformauspraegung_beschreibung_1,
            rechtsformauspraegung_beschreibung_2,
            COUNT(*) as count
        FROM kundenstamm 
        WHERE rechtsformauspraegung_beschreibung_1 IN ('AG', 'GmbH', 'OHG', 'KG')
        GROUP BY rechtsformauspraegung_beschreibung_1, rechtsformauspraegung_beschreibung_2
        ORDER BY count DESC
        """
        
        forms_result = db.cursor.execute(forms_query).fetchall()
        legal_forms = {}
        
        for row in forms_result:
            form_name = row['rechtsformauspraegung_beschreibung_1'] or row['rechtsformauspraegung_beschreibung_2'] or 'Sonstige'
            if form_name and form_name != 'None':
                legal_forms[form_name] = row['count']
        
    except Exception as e:
        st.error(f"Fehler beim Abrufen der Rechtsformen: {str(e)}")
        legal_forms = {
            'GmbH': 0,
            'AG': 0,
            'OHG/KG': 0,
            'Stiftung': 0,
            'Sonstige': 0
        }
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("📋 Stichprobenparameter nach Rechtsform")
        
        # Create input fields for each legal form
        sample_sizes = {}
        
        # Common legal forms
        common_forms = ['GmbH', 'AG', 'OHG', 'KG', 'Stiftung', 'eG', 'GmbH & Co. KG']
        
        for form in common_forms:
            # Find matching form in legal_forms
            matching_count = 0
            for db_form, count in legal_forms.items():
                if form.lower() in db_form.lower():
                    matching_count = count
                    break
            
            sample_sizes[form] = st.number_input(
                f"{form} (verfügbar: {matching_count})",
                min_value=0,
                max_value=1000,
                value=0,
                step=1,
                key=f"sample_{form}"
            )
        
        # Date range inputs
        st.subheader("📅 Zeitraum")
        date_col1, date_col2 = st.columns(2)
        
        with date_col1:
            start_date = st.date_input(
                "Von:",
                value=date(2020, 1, 1),
                format="DD.MM.YYYY",
                key="legal_start_date"
            )
        
        with date_col2:
            end_date = st.date_input(
                "Bis:",
                value=date.today(),
                format="DD.MM.YYYY",
                key="legal_end_date"
            )
        
        # Show accounts checkbox
        include_accounts = st.checkbox(
            "🔗 Mit Kontoinformationen verknüpfen (JOIN mit kontodaten_vw)",
            value=False,
            help="Zeigt zusätzliche Informationen aus der Kontodaten-Tabelle an"
        )
        
        # Apply button
        if st.button("🔍 Filter anwenden", type="primary", use_container_width=True, key="apply_legal"):
            with st.spinner("Ziehe Stichproben..."):
                all_results = []
                
                for form, size in sample_sizes.items():
                    if size > 0:
                        try:
                            # Build query for this legal form
                            if include_accounts:
                                # Query with JOIN to get account information
                                query = """
                                SELECT 
                                    k.*,
                                    COUNT(DISTINCT kd.kontonummer_pseudonym) as anzahl_konten,
                                    SUM(CASE WHEN kd.treuhandkonto = 'J' THEN 1 ELSE 0 END) as anzahl_treuhandkonten,
                                    SUM(CASE WHEN kd.anderkonto = 'J' THEN 1 ELSE 0 END) as anzahl_anderkonten,
                                    MIN(kd.kontoeroeffnung) as erstes_konto_datum
                                FROM kundenstamm k
                                LEFT JOIN kontodaten_vw kd 
                                    ON k.personennummer_pseudonym = kd.personennummer_pseudonym 
                                    AND k.banknummer = kd.banknummer
                                WHERE k.rechtsformauspraegung_beschreibung_1 IN ('AG', 'GmbH', 'OHG', 'KG')
                                AND (
                                    k.rechtsformauspraegung_beschreibung_1 LIKE ? 
                                    OR k.rechtsformauspraegung_beschreibung_2 LIKE ?
                                )
                                AND k.stichtag >= ?
                                AND k.stichtag <= ?
                                GROUP BY k.pk
                                ORDER BY RANDOM()
                                LIMIT ?
                                """
                            else:
                                # Simple query without JOIN
                                query = """
                                SELECT * FROM kundenstamm 
                                WHERE rechtsformauspraegung_beschreibung_1 IN ('AG', 'GmbH', 'OHG', 'KG')
                                AND (
                                    rechtsformauspraegung_beschreibung_1 LIKE ? 
                                    OR rechtsformauspraegung_beschreibung_2 LIKE ?
                                )
                                AND stichtag >= ?
                                AND stichtag <= ?
                                ORDER BY RANDOM()
                                LIMIT ?
                                """
                            
                            params = (
                                f'%{form}%',
                                f'%{form}%',
                                start_date.strftime('%Y-%m-%d'),
                                end_date.strftime('%Y-%m-%d'),
                                size
                            )
                            
                            result = db.cursor.execute(query, params).fetchall()
                            
                            if result:
                                df = pd.DataFrame([dict(row) for row in result])
                                df['_sampled_form'] = form
                                all_results.append(df)
                            
                        except Exception as e:
                            st.error(f"Fehler bei {form}: {str(e)}")
                
                if all_results:
                    # Combine all results
                    combined_df = pd.concat(all_results, ignore_index=True)
                    st.session_state.legal_entities_results = combined_df
                    st.success(f"✅ {len(combined_df)} Datensätze insgesamt gefunden!")
                else:
                    st.warning("Keine Datensätze gefunden.")
                    st.session_state.legal_entities_results = None
    
    with col2:
        st.subheader("📊 Statistik")
        
        # Show total requested samples
        total_requested = sum(sample_sizes.values())
        st.metric("Gesamt angeforderte Stichproben", total_requested)
        
        # Show breakdown by form
        if total_requested > 0:
            st.write("**Aufteilung:**")
            for form, size in sample_sizes.items():
                if size > 0:
                    st.write(f"- {form}: {size}")
    
    # Display results
    if hasattr(st.session_state, 'legal_entities_results') and st.session_state.legal_entities_results is not None:
        st.markdown("---")
        st.subheader("📋 Ergebnisse")
        
        df = st.session_state.legal_entities_results
        
        # Select columns to display
        display_columns = [
            '_sampled_form', 'kundennummer', 'banknummer', 'stichtag',
            'rechtsform', 'rechtsformauspraegung_beschreibung_1',
            'postleitzahl_pseudonym', 'ort_pseudonym'
        ]
        
        # Add account columns if JOIN was used
        if include_accounts and 'anzahl_konten' in df.columns:
            display_columns.extend([
                'anzahl_konten', 'anzahl_treuhandkonten', 
                'anzahl_anderkonten', 'erstes_konto_datum'
            ])
        
        # Filter available columns
        available_columns = [col for col in display_columns if col in df.columns]
        
        # Format dates
        if 'stichtag' in df.columns:
            df['stichtag'] = pd.to_datetime(df['stichtag'], dayfirst=True).dt.strftime('%d.%m.%Y')
        if 'erstes_konto_datum' in df.columns and 'erstes_konto_datum' in available_columns:
            df['erstes_konto_datum'] = pd.to_datetime(df['erstes_konto_datum'], dayfirst=True, errors='coerce').dt.strftime('%d.%m.%Y')
        
        # Display dataframe
        st.dataframe(
            df[available_columns],
            use_container_width=True,
            hide_index=True
        )
        
        # Show summary by form
        st.subheader("📊 Zusammenfassung nach Rechtsform")
        summary = df['_sampled_form'].value_counts()
        st.bar_chart(summary)
        
        # Show account statistics if JOIN was used
        if include_accounts and 'anzahl_konten' in df.columns:
            st.subheader("🏦 Kontoinformationen (aus JOIN)")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                total_accounts = df['anzahl_konten'].sum()
                st.metric("Gesamtanzahl Konten", f"{total_accounts:,}".replace(',', '.'))
            
            with col2:
                trust_accounts = df['anzahl_treuhandkonten'].sum()
                st.metric("Treuhandkonten", f"{trust_accounts:,}".replace(',', '.'))
            
            with col3:
                other_accounts = df['anzahl_anderkonten'].sum()
                st.metric("Anderkonten", f"{other_accounts:,}".replace(',', '.'))
        
        # Export button
        csv = df.to_csv(index=False, sep=';')
        st.download_button(
            label="📥 Als CSV exportieren",
            data=csv,
            file_name=f"stichprobe_juristische_personen_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

def show_about_page():
    """Display the about page with database tables overview"""
    st.title("ℹ️ Über das System")
    st.markdown("---")
    
    # Get database connection
    db = get_database()
    
    st.markdown("""
    ## Datenbanktabellen
    
    Das System verwendet drei Haupttabellen für die Stichprobenziehung:
    """)
    
    # Show each table
    tables = ['kundenstamm', 'softfact_vw', 'kontodaten_vw']
    
    for table in tables:
        st.subheader(f"📊 Tabelle: {table}")
        
        try:
            # Get row count
            count_result = db.cursor.execute(f"SELECT COUNT(*) as count FROM {table}").fetchone()
            row_count = count_result['count'] if count_result else 0
            
            # Get sample data
            sample_query = f"SELECT * FROM {table} LIMIT 10"
            sample_result = db.cursor.execute(sample_query).fetchall()
            
            if sample_result:
                df = pd.DataFrame([dict(row) for row in sample_result])
                
                # Show statistics
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Anzahl Datensätze", f"{row_count:,}".replace(',', '.'))
                with col2:
                    st.metric("Anzahl Spalten", len(df.columns))
                with col3:
                    st.metric("Beispieldaten", "10 Zeilen")
                
                # Show sample data
                st.dataframe(
                    df,
                    use_container_width=True,
                    hide_index=True,
                    height=200
                )
                
                # Show column info
                with st.expander(f"Spalteninformationen für {table}"):
                    col_info = db.get_column_info(table)
                    col_df = pd.DataFrame(
                        [(col, dtype) for col, dtype in col_info.items()],
                        columns=['Spaltenname', 'Datentyp']
                    )
                    st.dataframe(col_df, use_container_width=True, hide_index=True)
            else:
                st.warning(f"Keine Daten in Tabelle {table} gefunden.")
                
        except Exception as e:
            st.error(f"Fehler beim Lesen der Tabelle {table}: {str(e)}")
        
        st.markdown("---")
    
    # System information
    st.subheader("🖥️ Systeminformationen")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Version:** 2.0 (Streamlit)")
        st.write("**Datenbank:** SQLite")
        st.write("**Framework:** Streamlit")
    
    with col2:
        st.write("**Python Version:** 3.x")
        st.write("**Entwickelt für:** Finanzprüfung")
        st.write("**Lizenz:** Proprietär")

if __name__ == "__main__":
    main()