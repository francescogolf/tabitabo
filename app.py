"""
TABITABO - Table Metadata Management Application
A Streamlit application for Databricks Apps to simplify and accelerate 
database table field metadata management.
"""

import streamlit as st
import pandas as pd
from typing import List, Dict, Optional, Tuple
import Levenshtein


def calculate_similarity(col1: str, col2: str) -> int:
    """
    Calculate the Levenshtein distance between two column names.
    
    Args:
        col1: First column name
        col2: Second column name
    
    Returns:
        Levenshtein distance between the two strings
    """
    return Levenshtein.distance(col1.lower(), col2.lower())


def find_best_match(target_col: str, source_cols: List[str]) -> Optional[str]:
    """
    Find the best matching column from source columns for a target column.
    Returns the match only if the Levenshtein distance is <= 3.
    
    Args:
        target_col: Target column name
        source_cols: List of source column names
    
    Returns:
        Best matching column name or None if no match within threshold
    """
    best_match = None
    best_distance = float('inf')
    
    for source_col in source_cols:
        distance = calculate_similarity(target_col, source_col)
        if distance <= 3 and distance < best_distance:
            best_distance = distance
            best_match = source_col
    
    return best_match


def get_table_metadata(catalog: str, schema: str, table: str) -> pd.DataFrame:
    """
    Retrieve table metadata from Databricks.
    
    Args:
        catalog: Catalog name
        schema: Schema name
        table: Table name
    
    Returns:
        DataFrame with columns: column_name, comment
    """
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        
        # Get table description
        full_table_name = f"{catalog}.{schema}.{table}"
        describe_df = spark.sql(f"DESCRIBE TABLE {full_table_name}")
        
        # Extract column names and comments
        columns_data = []
        for row in describe_df.collect():
            # Skip partition information and other metadata
            if row['col_name'].startswith('#') or row['col_name'] == '':
                continue
            if row['col_name'] == '# Partition Information':
                break
                
            columns_data.append({
                'column_name': row['col_name'],
                'comment': row['comment'] if row['comment'] else ''
            })
        
        return pd.DataFrame(columns_data)
    except Exception as e:
        st.error(f"Error retrieving table metadata: {str(e)}")
        return pd.DataFrame(columns=['column_name', 'comment'])


def update_table_comments(catalog: str, schema: str, table: str, updates: pd.DataFrame):
    """
    Update table column comments in Databricks.
    
    Args:
        catalog: Catalog name
        schema: Schema name
        table: Table name
        updates: DataFrame with columns to update
    """
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        
        full_table_name = f"{catalog}.{schema}.{table}"
        
        for _, row in updates.iterrows():
            if row['sostituire']:
                col_name = row['colonna_tabo']
                new_comment = row['descrizione_proposta']
                
                # Escape single quotes in comments
                escaped_comment = new_comment.replace("'", "\\'")
                
                # Execute ALTER TABLE command
                sql = f"ALTER TABLE {full_table_name} ALTER COLUMN `{col_name}` COMMENT '{escaped_comment}'"
                spark.sql(sql)
        
        st.success(f"Successfully updated {len(updates[updates['sostituire']])} column comments!")
    except Exception as e:
        st.error(f"Error updating table comments: {str(e)}")


def create_decision_table(tabi_metadata: pd.DataFrame, tabo_metadata: pd.DataFrame) -> pd.DataFrame:
    """
    Create the decision table by comparing two tables' metadata.
    
    Args:
        tabi_metadata: Metadata from input table (tabI)
        tabo_metadata: Metadata from output table (tabO)
    
    Returns:
        DataFrame with the decision table structure
    """
    decision_data = []
    used_tabi_cols = set()
    
    for _, tabo_row in tabo_metadata.iterrows():
        tabo_col = tabo_row['column_name']
        tabo_comment = tabo_row['comment']
        
        # Find best match in tabI
        available_tabi_cols = [col for col in tabi_metadata['column_name'].tolist() 
                               if col not in used_tabi_cols]
        best_match = find_best_match(tabo_col, available_tabi_cols)
        
        tabi_col = ''
        tabi_comment = ''
        
        if best_match:
            used_tabi_cols.add(best_match)
            tabi_col = best_match
            tabi_comment = tabi_metadata[tabi_metadata['column_name'] == best_match]['comment'].iloc[0]
        
        # Determine proposed description
        if tabo_comment:
            descrizione_proposta = tabo_comment
        elif tabi_comment:
            descrizione_proposta = tabi_comment
        else:
            descrizione_proposta = ''
        
        decision_data.append({
            'sostituire': True,  # Default to "Sostituire"
            'colonna_tabo': tabo_col,
            'comment_tabo': tabo_comment,
            'colonna_tabi': tabi_col,
            'comment_tabi': tabi_comment,
            'descrizione_proposta': descrizione_proposta
        })
    
    return pd.DataFrame(decision_data)


def main():
    """Main application function."""
    st.set_page_config(
        page_title="TABITABO - Table Metadata Manager",
        page_icon="🏷️",
        layout="wide"
    )
    
    st.image("tabitabo.png", width=200)
    st.title("Table Metadata Manager")
    st.markdown("""
    Applicazione per semplificare la metadatazione dei campi delle tabelle di database,
    automatizzando la copia e l'adattamento delle descrizioni tra tabelle con struttura simile.
    """)
    
    # Initialize session state
    if 'decision_table' not in st.session_state:
        st.session_state.decision_table = None
    if 'tabo_info' not in st.session_state:
        st.session_state.tabo_info = None
    
    # Sidebar for table selection
    st.sidebar.header("📋 Selezione Tabelle")
    
    # Input Table (tabI) - Read Only
    st.sidebar.subheader("Tabella Input (tabI)")
    st.sidebar.caption("🔒 Sola lettura - origine dei metadati")
    tabi_catalog = st.sidebar.text_input("Catalog (tabI)", value="", key="tabi_catalog")
    tabi_schema = st.sidebar.text_input("Schema (tabI)", value="", key="tabi_schema")
    tabi_table = st.sidebar.text_input("Table (tabI)", value="", key="tabi_table")
    
    st.sidebar.divider()
    
    # Output Table (tabO) - Writable
    st.sidebar.subheader("Tabella Output (tabO)")
    st.sidebar.caption("✏️ Destinazione dei metadati")
    tabo_catalog = st.sidebar.text_input("Catalog (tabO)", value="", key="tabo_catalog")
    tabo_schema = st.sidebar.text_input("Schema (tabO)", value="", key="tabo_schema")
    tabo_table = st.sidebar.text_input("Table (tabO)", value="", key="tabo_table")
    
    # Analyze button
    if st.sidebar.button("🔍 ANALIZZA TABELLE", type="primary", use_container_width=True):
        if not all([tabi_catalog, tabi_schema, tabi_table, tabo_catalog, tabo_schema, tabo_table]):
            st.error("⚠️ Compilare tutti i campi per entrambe le tabelle!")
        else:
            with st.spinner("Caricamento metadati in corso..."):
                # Get metadata from both tables
                tabi_metadata = get_table_metadata(tabi_catalog, tabi_schema, tabi_table)
                tabo_metadata = get_table_metadata(tabo_catalog, tabo_schema, tabo_table)
                
                if not tabi_metadata.empty and not tabo_metadata.empty:
                    # Create decision table
                    st.session_state.decision_table = create_decision_table(tabi_metadata, tabo_metadata)
                    st.session_state.tabo_info = (tabo_catalog, tabo_schema, tabo_table)
                    st.success("✅ Analisi completata!")
                    st.rerun()
    
    # Main content area
    if st.session_state.decision_table is not None:
        st.header("📊 Tabella Decisionale")
        st.markdown("""
        Modifica la tabella sottostante per decidere quali descrizioni applicare alla tabella di output.
        - **Sostituire**: Seleziona se applicare la modifica
        - **Descrizione Proposta**: Campo modificabile per personalizzare la descrizione
        """)
        
        # Configure column display
        column_config = {
            'sostituire': st.column_config.CheckboxColumn(
                'Sostituire',
                help='Seleziona per applicare la modifica',
                default=True
            ),
            'colonna_tabo': st.column_config.TextColumn(
                'Nome Colonna (tabO)',
                help='Nome della colonna nella tabella di output',
                disabled=True
            ),
            'comment_tabo': st.column_config.TextColumn(
                'Comment Attuale (tabO)',
                help='Descrizione attuale nella tabella di output',
                disabled=True
            ),
            'colonna_tabi': st.column_config.TextColumn(
                'Nome Colonna (tabI)',
                help='Nome della colonna corrispondente nella tabella di input',
                disabled=True
            ),
            'comment_tabi': st.column_config.TextColumn(
                'Comment Origine (tabI)',
                help='Descrizione nella tabella di input',
                disabled=True
            ),
            'descrizione_proposta': st.column_config.TextColumn(
                'Descrizione Proposta',
                help='Descrizione che verrà applicata (modificabile)',
                required=False
            )
        }
        
        # Display editable data editor
        edited_df = st.data_editor(
            st.session_state.decision_table,
            column_config=column_config,
            use_container_width=True,
            num_rows="fixed",
            hide_index=True,
            key="decision_editor"
        )
        
        # Update session state with edited data
        st.session_state.decision_table = edited_df
        
        # Statistics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Totale Colonne", len(edited_df))
        with col2:
            st.metric("Da Sostituire", len(edited_df[edited_df['sostituire']]))
        with col3:
            matches = len(edited_df[edited_df['colonna_tabi'] != ''])
            st.metric("Corrispondenze Trovate", matches)
        
        # Confirm and action button
        st.divider()
        col_left, col_center, col_right = st.columns([1, 2, 1])
        with col_center:
            if st.button("✅ CONFERMA E AZIONE", type="primary", use_container_width=True):
                updates = edited_df[edited_df['sostituire']].copy()
                
                if len(updates) == 0:
                    st.warning("⚠️ Nessuna colonna selezionata per l'aggiornamento!")
                else:
                    with st.spinner(f"Aggiornamento di {len(updates)} colonne in corso..."):
                        tabo_catalog, tabo_schema, tabo_table = st.session_state.tabo_info
                        update_table_comments(tabo_catalog, tabo_schema, tabo_table, updates)
    else:
        # Welcome message
        st.info("""
        ### 👋 Benvenuto in TABITABO!
        
        **Come iniziare:**
        1. Inserisci i dettagli della **Tabella Input (tabI)** nella barra laterale - questa è la tabella sorgente dei metadati
        2. Inserisci i dettagli della **Tabella Output (tabO)** nella barra laterale - questa è la tabella da aggiornare
        3. Clicca su **ANALIZZA TABELLE** per generare la tabella decisionale
        4. Modifica le descrizioni proposte secondo necessità
        5. Clicca su **CONFERMA E AZIONE** per applicare le modifiche
        
        **Nota:** La tabella di input (tabI) è in sola lettura e non verrà modificata.
        """)


if __name__ == "__main__":
    main()
