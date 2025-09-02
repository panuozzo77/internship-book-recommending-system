# Sistema di Raccomandazione Libraria con Architettura Modulare

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://python.org)
[![MongoDB](https://img.shields.io/badge/MongoDB-4.4%2B-green)](https://mongodb.com)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## 📚 Descrizione

Sistema di raccomandazione libraria sviluppato come tesi di laurea triennale in Informatica presso l'Università degli Studi di Salerno. Il progetto implementa un'**architettura modulare per l'arricchimento dati** che integra fonti eterogenee per migliorare la qualità delle raccomandazioni attraverso tecniche ibride di machine learning.

### 🎯 Obiettivi del Progetto

- **Arricchimento Dinamico**: Superare le limitazioni dei dataset statici integrando metadati da fonti esterne
- **Architettura Estensibile**: Sistema a componenti basato su provider intercambiabili  
- **Raccomandazioni Ibride**: Combinazione di Content-Based e Collaborative Filtering
- **Scalabilità**: Utilizzo di indici ottimizzati (Annoy, FAISS) per performance elevate
- **Normalizzazione Intelligente**: Mappatura automatica dei generi tramite LLM (Gemma-3)

## 🏗️ Architettura del Sistema

Il sistema è strutturato in moduli specializzati che collaborano seguendo principi di ingegneria del software:

```
├── 📁 core/              # Sistema di inizializzazione e configurazione
├── 📁 etl/               # Extract, Transform, Load per preprocessing dati  
├── 📁 data_processing/   # Sistema di Data Augmentation con provider
├── 📁 recommender/       # Motori di raccomandazione ibridi
├── 📁 webapp/           # Interfaccia web per testing
└── 📁 testing/          # Suite di valutazione e performance testing
```

### 🔧 Componenti Principali

#### 1. **Sistema ETL** ([`etl/`](etl/))
- **Caricamento efficiente** di dataset di grandi dimensioni (29GB → 19GB)
- **Pulizia automatica** e deduplicate delle edizioni
- **Chunking intelligente** per ottimizzazioni di memoria
- **Indicizzazione strategica** per query veloci

#### 2. **Sistema Core** ([`core/`](core/))
- **Inizializzazione centralizzata** con pattern Singleton
- **Gestione configurazione** esternalizzata (JSON)
- **CLI avanzata** con subcomandi e routing automatico
- **Logging strutturato** multi-livello

#### 3. **Data Augmentation** ([`data_processing/`](data_processing/))
- **Architettura a Provider** estensibile (Strategy Pattern)
- **Aggregazione intelligente** con tracciamento fonti
- **Normalizzazione LLM** dei generi letterari
- **Provider implementati**:
  - OpenLibrary API
  - Google Books API
  - Goodreads Scraper (CLI wrapper)

#### 4. **Sistema di Raccomandazione** ([`recommender/`](recommender/))
- **Content-Based**: TF-IDF + Annoy per similarità semantica
- **Collaborative Filtering**: FAISS per ricerca utenti simili  
- **Re-ranking avanzato**: PageCount + GenrePreference
- **Profilazione utente**: Calcolo taste vector ponderato

#### 5. **WebApp** ([`webapp/`](webapp/))
- **Autenticazione** utenti (registrazione/login)
- **Gestione libreria** personale con rating
- **Ricerca libri** per titolo/autore
- **Visualizzazione raccomandazioni** personalizzate

## 📊 Dataset e Performance

### Dataset UCSD Goodreads
- **2.36M pubblicazioni** (1.52M opere uniche)  
- **876K utenti** attivi
- **228M interazioni** (recensioni e rating)
- **829K autori** e 400K serie

### Risultati di Performance
- **Precision@10**: 0.0061 (Content-Based), 0.0182 (Collaborative)  
- **Recall@10**: 0.0122 (Content-Based), 0.0363 (Collaborative)
- **Copertura Catalogo**: 0.85% con raccomandazioni diversificate
- **Valutazione Qualitativa**: 3.7/5 interesse medio (27 beta-tester)

## 🚀 Installazione e Configurazione

### Prerequisiti
- **Python 3.8+**
- **MongoDB 4.4+**
- **Ollama** (opzionale, per mappatura generi LLM)
- **8GB+ RAM** (per dataset completo)

### Setup Rapido

1. **Clona il repository**
```bash
git clone https://github.com/panuozzo77/internship-book-recommending-system.git
cd internship-book-recommending-system
```

2. **Installa le dipendenze**
```bash
pip install -r requirements.txt
```

3. **Configura MongoDB**
```bash
# Avvia MongoDB localmente
mongod --dbpath /path/to/your/db
```

4. **Crea configurazione**
```bash
cp config.json.example config.json
# Modifica config.json con i tuoi parametri
```

5. **Esegui ETL** (dataset richiesto)
```bash
python run.py etl --load-all
```

6. **Costruisci il modello**
```bash
python run.py tools --build-user-profiles
```

### Configurazione Avanzata

Il file [`config.json`](config.json.example) permette di personalizzare:
- **Connessione database** (URI, credenziali)
- **Percorsi dati** (dataset, modelli, log)
- **Logging** (livello, formato, file)
- **WebApp** (host, porta, debug)

## 🎮 Utilizzo

### Interfaccia CLI

```bash
# Carica tutti i dataset configurati
python run.py etl --load-all

# Raccomandazioni per titoli
python run.py recommend --by-title "Dune" "Foundation"

# Raccomandazioni per utente (Content-Based)  
python run.py recommend --by-user-id-content-based USER_ID

# Raccomandazioni per utente (Collaborative)
python run.py recommend --by-user-id-collaborative USER_ID

# Avvia interfaccia web
python run.py webui

# Costruisci profili utente e indice FAISS
python run.py tools --build-user-profiles

# Inferenza schema da dataset
python run.py tools --infer-schema --schema-input-dir ./data
```

### Interfaccia Web

Avvia il server web e naviga a `http://localhost:5001`:

```bash
python run.py webui
```

**Funzionalità disponibili:**
- 📝 Registrazione e autenticazione
- 📚 Gestione libreria personale  
- 🔍 Ricerca libri avanzata
- ⭐ Sistema di rating e recensioni
- 🎯 Raccomandazioni personalizzate

## 🧪 Testing e Valutazione

### Test di Performance Data Augmentation
```bash
cd testing/data_augmentation
python analyze_results.py
```

### Valutazione Sistema di Raccomandazione
```bash
cd testing/recommending_system  
python offline_evaluator.py
python run_enhanced_evaluation.py
```

### Metriche Implementate
- **Precision@k / Recall@k**: Accuratezza raccomandazioni
- **Genre Alignment**: Coerenza con preferenze generi
- **Coverage**: Diversità raccomandazioni nel catalogo
- **Jaccard Index**: Similarità mappatura generi

## 🔬 Tecnologie e Librerie

### Core Technologies
- **[Python](https://python.org)**: Linguaggio principale
- **[MongoDB](https://mongodb.com)**: Database NoSQL per flessibilità  
- **[Flask](https://flask.palletsprojects.com)**: Framework web minimale

### Machine Learning & NLP  
- **[Annoy](https://github.com/spotify/annoy)**: Ricerca approssimata vicini (Spotify)
- **[FAISS](https://faiss.ai)**: Ricerca vettoriale ad alte performance (Meta)
- **[scikit-learn](https://scikit-learn.org)**: TF-IDF e preprocessing
- **[Ollama](https://ollama.ai) + Gemma-3**: LLM locale per normalizzazione

### Data Processing
- **[pandas](https://pandas.pydata.org)**: Manipolazione dati strutturati
- **[pymongo](https://pymongo.readthedocs.io)**: Driver MongoDB Python
- **[requests](https://requests.readthedocs.io)**: Client HTTP per API

## 📈 Estensioni e Sviluppi Futuri

### 🎯 Roadmap Tecnica
- [ ] **Architettura Microservizi**: Scalabilità orizzontale
- [ ] **Deep Learning Models**: Integrazione neural embedding  
- [ ] **Real-time Updates**: Stream processing per nuovi dati
- [ ] **A/B Testing Framework**: Valutazione comparativa algoritmi

### 🌟 Nuove Funzionalità
- [ ] **Filtri Avanzati**: Lingua, formato, data pubblicazione
- [ ] **Gestione Serie**: Raccomandazioni sequence-aware  
- [ ] **Social Features**: Condivisione e community
- [ ] **API RESTful**: Interfaccia programmatica esterna

## 🤝 Contributi

I contributi sono benvenuti! Per contribuire:

1. **Fork** del repository
2. **Crea** un branch feature (`git checkout -b feature/AmazingFeature`)
3. **Commit** delle modifiche (`git commit -m 'Add AmazingFeature'`)
4. **Push** al branch (`git push origin feature/AmazingFeature`)  
5. **Apri** una Pull Request

### 🐛 Bug Reports

Utilizza [GitHub Issues](https://github.com/panuozzo77/internship-book-recommending-system/issues) per segnalare bug o richiedere nuove funzionalità.

## 📄 Licenza

Questo progetto è distribuito sotto licenza **MIT**. Vedi [`LICENSE`](LICENSE) per dettagli.

## 👨‍💻 Autore

**Cristian Porzio**  
📧 Email: [cristian.porzio@studenti.unisa.it](mailto:cristian.porzio@studenti.unisa.it)  
🎓 Corso di Laurea Triennale in Informatica  
🏛️ Università degli Studi di Salerno  
📅 Anno Accademico 2024-2025

### 👨‍🏫 Relatore
**Prof. Fabio Palomba**  
🏛️ Università degli Studi di Salerno

## 📚 Citazioni e Riferimenti

```bibtex
@thesis{porzio2025architettura,
  title={Architettura Modulare per l'Arricchimento Dati in Sistemi di Raccomandazione Libraria},
  author={Porzio, Cristian},
  school={Università degli Studi di Salerno},
  year={2025},
  type={Tesi di Laurea Triennale},
  url={https://github.com/panuozzo77/internship-book-recommending-system}
}
```

### 📖 Dataset

Il progetto utilizza il dataset **UCSD Goodreads**:
- Wan, M., & McAuley, J. (2018). Item recommendation on monotonic behavior chains. *RecSys 2018*.
- Dataset disponibile: [UCSD Goodreads Dataset](https://sites.google.com/eng.ucsd.edu/ucsdbookgraph/home)

---

⭐ **Se questo progetto ti è stato utile, lascia una stella!** ⭐