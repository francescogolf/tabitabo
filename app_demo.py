"""
TABITABO - Table Metadata Management Application (Demo Mode)
Demo version without Databricks dependency for local testing.
"""

import streamlit as st
import pandas as pd
from typing import List, Optional
import Levenshtein


def calculate_similarity(col1: str, col2: str) -> int:
    """Calculate the Levenshtein distance between two column names."""
    return Levenshtein.distance(col1.lower(), col2.lower())


def find_best_match(target_col: str, source_cols: List[str]) -> Optional[str]:
    """
    Find the best matching column from source columns for a target column.
    Returns the match only if the Levenshtein distance is <= 3.
    """
    best_match = None
    best_distance = float('inf')
    
    for source_col in source_cols:
        distance = calculate_similarity(target_col, source_col)
        if distance <= 3 and distance < best_distance:
            best_distance = distance
            best_match = source_col
    
    return best_match


def get_demo_table_metadata(table_name: str) -> pd.DataFrame:
    """Get demo table metadata for testing."""
    demo_tables = {
        "customers": pd.DataFrame({
            'column_name': ['customer_id', 'first_name', 'last_name', 'email', 'phone_number', 'created_at'],
            'comment': ['Unique customer identifier', 'Customer first name', 'Customer last name', 
                       'Customer email address', 'Contact phone number', 'Account creation timestamp']
        }),
        "users": pd.DataFrame({
            'column_name': ['user_id', 'firstname', 'lastname', 'email_addr', 'phone', 'registration_date', 'status'],
            'comment': ['', '', '', '', '', '', 'Account status (active/inactive)']
        }),
        "products": pd.DataFrame({
            'column_name': ['product_id', 'product_name', 'description', 'price', 'category', 'stock_quantity'],
            'comment': ['Unique product identifier', 'Product display name', 'Product description', 
                       'Price in USD', 'Product category', 'Available stock quantity']
        }),
        "items": pd.DataFrame({
            'column_name': ['item_id', 'item_name', 'desc', 'price', 'category_id', 'qty'],
            'comment': ['', '', '', '', '', '']
        })
    }
    
    if table_name.lower() in demo_tables:
        return demo_tables[table_name.lower()]
    else:
        return pd.DataFrame(columns=['column_name', 'comment'])


def create_decision_table(tabi_metadata: pd.DataFrame, tabo_metadata: pd.DataFrame) -> pd.DataFrame:
    """Create the decision table by comparing two tables' metadata."""
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
            'sostituire': True,
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
        page_title="TABITABO - Table Metadata Manager (Demo)",
        page_icon="🏷️",
        layout="wide"
    )
    
    st.title("🏷️ TABITABO - Table Metadata Manager")
    st.info("🧪 **Demo Mode** - Using sample data for testing")
    st.markdown("""
    Applicazione per semplificare la metadatazione dei campi delle tabelle di database,
    automatizzando la copia e l'adattamento delle descrizioni tra tabelle con struttura simile.
    """)
    
    # Initialize session state
    if 'decision_table' not in st.session_state:
        st.session_state.decision_table = None
    if 'tabo_table' not in st.session_state:
        st.session_state.tabo_table = None
    
    # Sidebar for table selection
    st.sidebar.header("📋 Selezione Tabelle")
    st.sidebar.caption("Demo tables: customers, users, products, items")
    
    # Input Table (tabI) - Read Only
    st.sidebar.subheader("Tabella Input (tabI)")
    st.sidebar.caption("🔒 Sola lettura - origine dei metadati")
    tabi_table = st.sidebar.selectbox(
        "Select Input Table (tabI)", 
        ["", "customers", "products"],
        key="tabi_table"
    )
    
    st.sidebar.divider()
    
    # Output Table (tabO) - Writable
    st.sidebar.subheader("Tabella Output (tabO)")
    st.sidebar.caption("✏️ Destinazione dei metadati")
    tabo_table = st.sidebar.selectbox(
        "Select Output Table (tabO)", 
        ["", "users", "items"],
        key="tabo_table"
    )
    
    # Analyze button
    if st.sidebar.button("🔍 ANALIZZA TABELLE", type="primary", use_container_width=True):
        if not tabi_table or not tabo_table:
            st.error("⚠️ Selezionare entrambe le tabelle!")
        else:
            with st.spinner("Caricamento metadati in corso..."):
                # Get metadata from both tables
                tabi_metadata = get_demo_table_metadata(tabi_table)
                tabo_metadata = get_demo_table_metadata(tabo_table)
                
                if not tabi_metadata.empty and not tabo_metadata.empty:
                    # Create decision table
                    st.session_state.decision_table = create_decision_table(tabi_metadata, tabo_metadata)
                    st.session_state.tabo_table = tabo_table
                    st.success("✅ Analisi completata!")
                    st.rerun()
                else:
                    st.error("❌ Tabella non trovata nei dati demo!")
    
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
                disabled=True,
                width="medium"
            ),
            'comment_tabo': st.column_config.TextColumn(
                'Comment Attuale (tabO)',
                help='Descrizione attuale nella tabella di output',
                disabled=True,
                width="medium"
            ),
            'colonna_tabi': st.column_config.TextColumn(
                'Nome Colonna (tabI)',
                help='Nome della colonna corrispondente nella tabella di input',
                disabled=True,
                width="medium"
            ),
            'comment_tabi': st.column_config.TextColumn(
                'Comment Origine (tabI)',
                help='Descrizione nella tabella di input',
                disabled=True,
                width="medium"
            ),
            'descrizione_proposta': st.column_config.TextColumn(
                'Descrizione Proposta',
                help='Descrizione che verrà applicata (modificabile)',
                required=False,
                width="large"
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
                    st.success(f"""
                    ✅ **Demo Mode**: In produzione, verrebbero aggiornate {len(updates)} colonne nella tabella '{st.session_state.tabo_table}'
                    
                    **Colonne da aggiornare:**
                    """)
                    
                    for _, row in updates.iterrows():
                        st.write(f"- `{row['colonna_tabo']}`: \"{row['descrizione_proposta']}\"")
    else:
        # Welcome message
        st.info("""
        ### 👋 Benvenuto in TABITABO!
        
        **Come iniziare (Demo Mode):**
        1. Seleziona la **Tabella Input (tabI)** nella barra laterale (esempio: customers o products)
        2. Seleziona la **Tabella Output (tabO)** nella barra laterale (esempio: users o items)
        3. Clicca su **ANALIZZA TABELLE** per generare la tabella decisionale
        4. Modifica le descrizioni proposte secondo necessità
        5. Clicca su **CONFERMA E AZIONE** per vedere il risultato
        
        **Tabelle Demo Disponibili:**
        - **customers** → **users**: Tabella clienti con colonne simili
        - **products** → **items**: Tabella prodotti con colonne simili
        
        **Nota:** Questa è una versione demo. In produzione, l'app si connetterà a Databricks.
        """)


if __name__ == "__main__":
    main()
