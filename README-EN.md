# GTFS2NeTEx-converter USER MANUAL

# INDEX

[1 COMPONENTS](#1-components)

[2 REQUIREMENTS](#2-requirements)

[3 PREPARATION OF THE ENVIRONMENT](#3-preparation-of-the-environment)

[4 INPUT DATA](#4-input-data)

[5 RUN](#5-run)

[6 TODO ](#6-todo)


---

# 1 COMPONENTS  
The components released are as follows:

- **GTFS2NeTEx-converter** : software for converting GTFS feeds to NeTEx Italian Profile Level 1 (EPIP) according to [Schema NeTEx Italian Profile](https://github.com/5Tsrl/netex-italian-profile) and the corresponding [Linee Guida di Compilazione](https://github.com/5Tsrl/netex-italian-profile/tree/main/Linee%20guida)



GTFS2NeTEx-converter is a command-line application, organized into modules containing various classes and methods.

---


# 2 REQUIREMENTS
The software requires installation of:

- **Python 3.10** o superiore

The following libraries must be installes (*pip install* o *pip3 install*):

- **pandas**
- **haversine**

GTFS2NeTEx-converter uses SQLite3 in the processing stage; the SQLite3 environment is generated at runtime by the corresponding Python 3 base library: no installation of a RDBMS is therefore necessary.

We recommend using with machines equipped with at least 16GB RAM.

---

# 3 PREPARATION OF THE ENVIRONMENT
Clone the git project to a dedicated directory ```<DEV_HOME>```

```cd <DEV_HOME>```

```git clone https://github.com/Liguria-Digitale/GTFS2NeTEx-converter.git```

---

# 4 INPUT DATA

## 4.1 WARNINGS
GTFS is a *specification*, a *de facto standard* and not a *de iure standard* like NeTEx.

Therefore - not having a control scheme - GTFS can be subject to multiple implementations, while maintaining a basic structure adhering to the official specification.

For this reason **GTFS2NeTEx-converter** is implemented strictly adhering to the [General Transit Feed Specification](https://gtfs.org/schedule/), using the NeTEx Italian Profile scheme.

**Therefore, the following are not processed:**
- files .txt not covered by the specification
- fields not provided for by the specification within the .txt files covered by the specification

Where processing of such information is necessary, the code should be customized.

A complete description of the *File Requirements* is provided in [GTFS Schedule Reference](https://gtfs.org/schedule/reference/)

The program in its current version checks for files:

- agency.txt
- calendar_dates.txt (*)
- calendar.txt (*)
- feed_info.txt
- routes.txt
- stops.txt
- trips.txt
- stop_times.txt
- shapes.txt

(*) ```calendar_dates.txt``` and ```calendar.txt``` may be present as an alternative or in combination.

**It is also noted that to achieve an optimal conversion:**
- only *single-agency* GTFS feeds are processed (i.e. feeds in which the file ```agency.txt``` contains only one record)
- the file ```feed_info.txt``` must be present, with fields properly valued
- in the file ```stop_times.txt``` it must be present and correctly valued field ``shape_dist_traveled`: **if condition is not met the program ends**
- the file ```shapes.txt``` must be present:
    - if ```shapes.txt``not available **the program ends**: we recommend using the tool *Pfaedle* (see 4.2.1)
    - if ```shapes.txt``is available but the field ``shape_dist_traveled``is not valued, **GTFS2NeTEx-converter** performs the calculation of distances in the conversion phase using the [haversines formula](https://en.wikipedia.org/wiki/Haversine_formula)


---

## 4.2 DATA PRE-PROCESSING


### 4.2.1 Tools for validation and control of the GTFS feed
We recommend that you pre-process the GTFS feed to verify its correctness and compliance with the specification.

Examples of specific validation tools:

- [Transitfeed](https://github.com/google/transitfeed)

    Transitfeed is a collection of Python 2.7 applications that allow to perform formal validation and GTFS control operations in input (on the same machine can coexist multiple versions of Python 2 and Python 3).
    Transitfeed applications are also decribed in [Transitfeed Wiki](https://github.com/google/transitfeed/wiki) 


- [Mobility Data GTFS Validator](https://github.com/MobilityData/gtfs-validator?tab=readme-ov-file)

    GTFS Validator is available as:
    - web service
    - desktop application

Both tools also report *warnings* about the following issues:
- *Too Fast Travel* caused by:
    - Consecutive stops placed too far apart (typically it is an error of positioning these stops)
    - Incorrect travel time (too short) compared to the distance between consecutive stops

- *Too Many Consecutive Stop Times With Same Time* probably due to time interpolation errors of consecutive stops by the program generating the GTFS feed 

These problems do not affect the process of conversion into NeTEx (and subsequent formal validation) but may generate errors during the import of the NeTEx file in travel planning engines such as **OpenTripPlanner**, so their correction is strongly recommended.  


### 4.2.1 GTFS Feed Upgrade Tools

To remedy the lack of the file ``shapes.txt``it is recommended to use: 

- [Pfaedle](https://github.com/ad-freiburg/pfaedle)

    Pfaedle is a C++ application developed and maintained by the *Algorithms and Data Structures Group* of the University of Fribourg.
    
    Pfaedle allows for an accurate *map-matching* of a GTFS feed in input using a file ```.osm```(**OpenStreetMap**) for the geographical area concerned: the correct transport network is automatically selected by Pfaedle on the basis of the value ```route_type``` of ```routes.txt```.

    .osm files for Italy can be downloaded free of charge from [Geofabrik Download Server ](https://download.geofabrik.de/europe/italy.html).
    
    The output GTFS feed contains all the files of the input GTFS feed with the following upgrades:
    - the file ```stop_times.txt``` with the field ```shape_dist_traveled``` valued, thus allowing to draw *JourneyPatterns* from the GTFS feed.
    - the file ```shapes.txt``` with the field ```shape_dist_traveled``` valued, allowing to calculate distances in NeTEx.

    **Pfaedle can also be used in containerized mode (*Dockerfile*), highly recommended.**

---
# 5 RUN

In Windows open a Command Window **as Administrator** (in Linux/MacOS environments open a Terminal Window with **root privileges**):


```cd <DEV_HOME>```

The converter runs with the command::

```python GTFS2NeTEx-converter.py --folder <GTFS_FEED_FOLDER> --NUTS <NUTS2_CODE> --db <AGENCY_ACRONYM> --az <AGENCY_ACRONYM> --vat <AGENCY_VAT_NUMBER> --version <VERSION_NAME>```


The meaning of the arguments (all mandatory) is as follows:
- ```--folder <GTFS_FEED_FOLDER>```: pointing to the absolute path of the **directory where the GTFS feed was previously uncompressed**
- ```--NUTS <NUTS2_CODE>```: NUTS2 code of the territory in which the transportation agency operates (the complete list of NUTS2 codes for Italy can be found in [Elenco NUTS2 italiani](https://it.wikipedia.org/wiki/Nomenclatura_delle_unit%C3%A0_territoriali_per_le_statistiche_dell%27Italia))
- ```--db <AGENCY_ACRONYM>```: name of the intermediate SQLite3 database used by the converter (*avoid blank and special characters*)
- ```--az <AGENCY_ACRONYM>```: acronym of the transportation agency (*avoid blank and special characters*): it will be used to structure the data in NeTEx
- ```--vat <AGENCY_VAT_NUMBER>```: VAT number of the company TPL (*11 numeric characters*)
- ```--version <VERSION_NAME>```: name of the output NeTEx version (*avoid blank and special characters*)

An example:
*```python GTFS2NeTEx-converter.py --folder D:\APPO-OPENTRIPPLANNER\DATI\GTFS-feeds-RL\BASE-TPLLINEA\GTFS-IT-ITC3-TPLLINEA-20240701-20240908-pf-fares --NUTS IT:ITC3 --db TPLLINEA --az TPLLINEA --vat 01556040093 --version 240202```*

If the conversion process ends correctly within the directory ```<GTFS_FEED_FOLDER>``` the following files will be present:

- ```<NUTS2_CODE>-<AGENCY_ACRONYM>-NeTEx_L1.xml```: NeTEx Italian Profile Level 1 (uncompressed)
- ```<NUTS2_CODE>-<AGENCY_ACRONYM>-NeTEx_L1.xml.gz```: NeTEx Italian Profile Level 1 (gz-compressed)
- ```<AGENCY_ACRONYM>.db```: Intermediate SQLite3 database used by the converter during processing; can be used for debugging purposes and processed later for further purposes (e.g. calculation of transportation KPIs related to the transportation offer described in the data); an effective cross-platform open source editor for SQLite is [DB Browser for SQLite](https://sqlitebrowser.org/) 





---

