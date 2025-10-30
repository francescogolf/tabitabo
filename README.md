# 🏷️ TABITABO - Table Metadata Manager

TABITABO è un'applicazione Python basata su Streamlit, progettata per Databricks Apps, che semplifica e accelera la metadatazione dei campi delle tabelle di database automatizzando la copia e l'adattamento delle descrizioni tra tabelle con struttura simile.

## 🎯 Obiettivo

L'applicazione permette di copiare e adattare automaticamente i metadati (commenti) delle colonne da una tabella di origine (tabI) a una tabella di destinazione (tabO), identificando le corrispondenze tra colonne con nomi simili.

## ⚙️ Funzionamento

### 1. Analisi e Confronto
- L'app confronta le colonne di tabI (tabella input) e tabO (tabella output)
- Trova corrispondenze tra colonne con nomi uguali o che differiscono per massimo 3 caratteri (usando la distanza di Levenshtein)
- Ogni colonna di tabO può avere al massimo una corrispondenza in tabI (la più simile)

### 2. Visualizzazione Risultati
L'applicazione mostra una **Tabella Decisionale** con 6 campi:

1. **Sostituire** (booleano) - Modificabile dall'utente
   - Default: true (selezionato)
   - Indica se applicare la modifica alla colonna

2. **Nome Colonna (tabO)** - Non modificabile
   - Nome della colonna nella tabella di destinazione

3. **Comment Attuale (tabO)** - Non modificabile
   - Commento attualmente presente sulla colonna di tabO

4. **Nome Colonna (tabI)** - Non modificabile
   - Nome della colonna corrispondente in tabI (se trovata)
   - Vuoto se non c'è corrispondenza

5. **Comment Origine (tabI)** - Non modificabile
   - Commento della colonna in tabI (se trovata corrispondenza)
   - Vuoto se non c'è corrispondenza

6. **Descrizione Proposta** - Modificabile dall'utente
   - Logica di popolamento:
     - Se esiste un commento su tabO, copia quello
     - Altrimenti, se esiste un commento su tabI, copia quello
     - Altrimenti lascia vuoto

### 3. Conferma e Applicazione
- Il pulsante **"CONFERMA E AZIONE"** applica le modifiche
- Aggiorna solo le descrizioni delle colonne di tabO per cui il campo "Sostituire" è selezionato
- **Nota importante**: La tabI (tabella input) è in sola lettura e non viene mai modificata

## 📋 Requisiti

- Python 3.8+
- Streamlit
- Pandas
- python-Levenshtein
- Databricks Runtime (per l'accesso ai cataloghi e alle tabelle)

## 🚀 Installazione

1. Clona il repository:
```bash
git clone <repository-url>
cd tabitabo
```

2. Installa le dipendenze:
```bash
pip install -r requirements.txt
```

## 💻 Utilizzo

### Su Databricks Apps

1. Carica l'applicazione su Databricks
2. Configura come Databricks App
3. Avvia l'applicazione

### Localmente (per sviluppo)

```bash
streamlit run app.py
```

### Workflow

1. **Inserisci i dettagli della Tabella Input (tabI)**:
   - Catalog
   - Schema
   - Table name

2. **Inserisci i dettagli della Tabella Output (tabO)**:
   - Catalog
   - Schema
   - Table name

3. **Clicca su "ANALIZZA TABELLE"**:
   - L'app caricherà i metadati
   - Genererà automaticamente le corrispondenze
   - Mostrerà la tabella decisionale

4. **Rivedi e modifica**:
   - Deseleziona "Sostituire" per le colonne che non vuoi modificare
   - Modifica le descrizioni proposte se necessario

5. **Applica le modifiche**:
   - Clicca su "CONFERMA E AZIONE"
   - Le modifiche verranno applicate a tabO

## 📊 Funzionalità

### Fase 1: Selezione Tabelle
- **Input utente**: Scelta di tabI e tabO
- **Output**: Tabelle caricate

### Fase 2: Analisi Corrispondenze
- **Input**: Nessuno (automatico)
- **Output**: Proposte generate automaticamente

### Fase 3: Revisione
- **Input utente**: Modifica dei campi modificabili
- **Output**: Tabella decisionale personalizzata

### Fase 4: Conferma
- **Input utente**: Click su "CONFERMA E AZIONE"
- **Output**: Metadati aggiornati su tabO

## 🔒 Note di Sicurezza

- La tabella di input (tabI) è protetta in sola lettura
- Solo la tabella di output (tabO) viene modificata
- Le modifiche sono applicate solo alle colonne selezionate dall'utente

## 🛠️ Tecnologie Utilizzate

- **Streamlit**: Framework per l'interfaccia utente
- **Pandas**: Manipolazione dei dati
- **Levenshtein**: Calcolo della similarità tra stringhe
- **PySpark**: Interazione con Databricks

## 📝 Licenza

[Specificare la licenza]

## 👥 Contributi

[Istruzioni per contribuire al progetto]
