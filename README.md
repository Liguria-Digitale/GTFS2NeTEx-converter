# MANUALE GTFS2NeTEx-converter

# INDICE

[1 COMPONENTI](#1-componenti)

[2 REQUISITI](#2-requisiti)

[3 PREPARAZIONE AMBIENTE](#3-preparazione-ambiente)

[4 I DATI IN INGRESSO](#4-i-dati-in-ingresso)

[5 RUN](#5-run)

[6 TODO ](#6-todo)


---

# 1 COMPONENTI  
I componenti rilasciati sono i seguenti:

- **GTFS2NeTEx-converter** : software per la conversione di feed GTFS in NeTEx Italian Profile Level 1 (EPIP) secondo lo [Schema NeTEx Italian Profile](https://github.com/5Tsrl/netex-italian-profile) e le corrispondenti [Linee Guida di Compilazione](https://github.com/5Tsrl/netex-italian-profile/tree/main/Linee%20guida)



GTFS2NeTEx-converter e' un applicativo a linea di comando, organizzato in moduli che contengono varie classi e metodi.

---


# 2 REQUISITI
Il software richiede l'installazione di:

- **Python 3.10** o superiore

Devono inoltre essere installate (*pip install* o *pip3 install*) le librerie:

- **pandas**
- **haversine**

GTFS2NeTEx-converter utilizza SQLite3 in fase di elaborazione; l'ambiente SQLite3 e' generato in fase di runtime dalla corrispondente libreria base Python 3: non e' pertanto necessaria alcuna forma di installazione di un RDBMS.

Si consiglia l'utilizzo con macchine dotate di almeno 16GB RAM.

---

# 3 PREPARAZIONE AMBIENTE
Clonare il progetto git in una apposita directory ```<DEV_HOME>```

```cd <DEV_HOME>```

```git clone https://github.com/Liguria-Digitale/GTFS2NeTEx-converter.git```

---

# 4 I DATI IN INGRESSO

## 4.1 AVVERTENZE
GTFS e' una *specifica*, uno *standard de facto* e non uno *standard de iure* come NeTEx.

Non avendo uno schema di controllo puo' essere soggetto a molteplici implementazioni, pur mantenendo una struttura base aderente alla specifica ufficiale.

Per questa ragione **GTFS2NeTEx-converter** e' implementato aderendo strettamente alla specifica [General Transit Feed Specification](https://gtfs.org/schedule/), traguardando lo schema NeTEx Italian Profile.

**Pertanto non vengono elaborati:**
- files .txt non contemplati dalla specifica
- campi non previsti dalla specifica all'interno dei files .txt contemplati dalla specifica

Laddove si rendesse necessaria l'elaborazione di tali informazioni il codice dovra' essere personalizzato.

Una descrizione esaustiva dei *File Requirements* e' fornita nel [GTFS Schedule Reference](https://gtfs.org/schedule/reference/)

Il programma nella versione attuale controlla l'esistenza dei files:

- agency.txt
- calendar_dates.txt (*)
- calendar.txt (*)
- feed_info.txt
- routes.txt
- stops.txt
- trips.txt
- stop_times.txt
- shapes.txt

(*) calendar_dates.txt e calendar.txt possono essere presenti in alternativa o in combinazione.

**Si avverte inoltre che per ottenere una conversione ottimale:**
- vengono trattati solamente feed GTFS *monoziendali* (ovvero feed in cui il file ```agency.txt``` contiene un solo record)
- deve essere presente il file ```feed_info.txt```, correttamente compilato
- nel file ```stop_times.txt``` deve essere presente e correttamente valorizzato il campo ```shape_dist_traveled```: **qualora la condizione non sia verificata il programma termina** 
- deve essere presente il file ```shapes.txt```:
    - se ```shapes.txt``` non e' disponibile **il programma termina**: si consiglia l'utilizzo preliminare del tool *Pfaedle* (vedi 4.2.1)
    - se ```shapes.txt``` e' disponibile ma il campo ```shape_dist_traveled``` non e' valorizzato, **GTFS2NeTEx-converter** effettua il calcolo delle distanze in fase di conversione utilizzando la [formula degli haversines](https://en.wikipedia.org/wiki/Haversine_formula)


---

## 4.2 PREPARAZIONE DEI DATI


### 4.2.1 Tools di validazione e controllo del feed GTFS
Si consiglia di effettuare un pre-processing del feed GTFS, allo scopo di verificarne la correttezza e la rispondenza alla specifica.

Esempi di tools specifici di validazione:

- [Transitfeed](https://github.com/google/transitfeed)

    Transitfeed e' una collezione di applicazioni Python 2.7 che permettono di effettuare operazioni di validazione formale e controllo del GTFS in input (sulla stessa macchina possono coesistere molteplici versioni di Python 2 e Python 3).
    I vari applicativi sono descritti anche in [Transitfeed Wiki](https://github.com/google/transitfeed/wiki) 


- [Mobility Data GTFS Validator](https://github.com/MobilityData/gtfs-validator?tab=readme-ov-file)

    GTFS Validator e' disponibile come:
    - servizio web
    - come applicazione desktop

Entrambi i tools descritti segnalano anche *warnings* relativi ai seguenti problemi:
- *Too Fast Travel* imputabili alle seguenti cause:
    - Fermate consecutive posizionate troppo distanti tra loro (tipicamente e' un errore di posizionamento delle fermate stesse)
    - Intertempi errati (troppo brevi) rispetto alla distanza tra fermate consecutive

- *Too Many Consecutive Stop Times With Same Time* probabilmente imputabili ad errori di interpolazione temporale delle fermate consecutive da parte del sistema che genera il GTFS 

Questi problemi non inficiano il processo di conversione in NeTEx (e la successiva validazione formale) ma possono generare errori in fase di import del NeTEx prodotto in ambienti di travel planning quali **OpenTripPlanner**, pertanto la loro correzione e' fortemente consigliata.  


### 4.2.1 Tools di upgrade del feed GTFS

Per ovviare alla mancanza del file ```shapes.txt``` si consiglia l'utilizzo di: 

- [Pfaedle](https://github.com/ad-freiburg/pfaedle)

    Pfaedle e' una applicazione C++ sviluppata e mantenuta dal *Algorithms and Data Structures Group* dell'Universita' di Friburgo.
    
    Pfaedle permette di effettuare un *map-matching* accurato di un feed GTFS in input utilizzando un file ```.osm``` (**OpenStreetMap**) relativo all'area geografica interessata: la rete di trasporto corretta e' scelta automaticamente da Pfaedle in base al valore ```route_type``` di ```routes.txt```.

    I files .osm per l'Italia sono scaricabili dal [Geofabrik Download Server ](https://download.geofabrik.de/europe/italy.html).
    
    Il feed GTFS generato contiene:
    - il file ```stop_times.txt``` con il campo ```shape_dist_traveled``` valorizzato, consentendo di dedurre i *JourneyPatterns* dal GTFS.
    - il file ```shapes.txt``` con il campo ```shape_dist_traveled``` valorizzato, consentendo di calcolare le estensioni delle distanze in NeTEx.

    **Pfaedle e' anche utilizzabile in modalita' containerizzata (*Dockerfile*), altamente consigliabile.**

---
# 5 RUN

In ambienti Windows aprire una Command Window **come Amministratore** (in ambienti Linux/MacOS aprire una Terminal Window con **privilegi di root**):


```cd <DEV_HOME>```

Il convertitore e' lanciato con il comando:

```python GTFS2NeTEx-converter.py --folder <GTFS_FEED_FOLDER> --NUTS <NUTS2_CODE> --db <AGENCY_ACRONYM> --az <AGENCY_ACRONYM> --vat <AGENCY_VAT_NUMBER> --version <VERSION_NAME>```


Il significato degli argomenti (tutti obbligatori) e' il seguente:
- ```--folder <GTFS_FEED_FOLDER>```: puntamento alla path assoluta della **directory ove il feed GTFS e' stato precedentemente decompresso**
- ```--NUTS <NUTS2_CODE>```: codice NUTS2 del territorio in cui e' compreso il bacino di esercizio dell'azienda di trasporto (l'elenco completo dei codici NUTS2 per l'Italia e' consultabile in [Elenco NUTS2 italiani](https://it.wikipedia.org/wiki/Nomenclatura_delle_unit%C3%A0_territoriali_per_le_statistiche_dell%27Italia))
- ```--db <AGENCY_ACRONYM>```: nome del database SQLite3 intermedio utilizzato dal convertitore (*evitare blank e caratteri speciali*)
- ```--az <AGENCY_ACRONYM>```: acronimo dell'azienda TPL (*evitare blank e caratteri speciali*) che verra' utilizzato per strutturare i dati in NeTEx
- ```--vat <AGENCY_VAT_NUMBER>```: numero di Partita IVA dell'azienda TPL (*11 caratteri numerici*)
- ```--version <VERSION_NAME>```: nome della versione del NeTEx prodotto (*evitare blank e caratteri speciali*)

Esempio di utilizzo:
*```python GTFS2NeTEx-converter.py --folder D:\APPO-OPENTRIPPLANNER\DATI\GTFS-feeds-RL\BASE-TPLLINEA\GTFS-IT-ITC3-TPLLINEA-20240701-20240908-pf-fares --NUTS IT:ITC3 --db TPLLINEA --az TPLLINEA --vat 01556040093 --version 240202```*

Se il processo termina correttamente all'interno della directory ```<GTFS_FEED_FOLDER>``` saranno presenti i seguenti files:

- ```<NUTS2_CODE>-<AGENCY_ACRONYM>-NeTEx_L1.xml```: file NeTEx Italian Profile Level 1 (non compresso)
- ```<NUTS2_CODE>-<AGENCY_ACRONYM>-NeTEx_L1.xml.gz```: stesso file NeTEx Italian Profile Level 1 (in formato gz compresso)
- ```<AGENCY_ACRONYM>.db```: database SQLite3 intermedio utilizzato dal convertitore durante l'elaborazione; puo' essere usato sia per scopi di debugging che elaborato successivamente per altre finalita' (es. calcolo KPI relativi all'offerta di trasporto descritta nei dati); un efficace editor multipiattaforma open source per SQLite è' [DB Browser for SQLite](https://sqlitebrowser.org/) 

## Lanciare come docker container
L'applicazione è anche disponibile in formato docker (vedi sezione packages)

La cartella gtfs deve essere mountata come volume, e passata come `<GTFS_FEED_FOLDER>`:
```sh
docker run --rm --volume ./gtfs:/gtfs  ghcr.io/liguria-digitale/gtfs2netex-converter:latest --folder /gtfs --NUTS <NUTS2_CODE> --db <AGENCY_ACRONYM> --az <AGENCY_ACRONYM> --vat <AGENCY_VAT_NUMBER> --version <VERSION_NAME>
```

