# bento-mdb-external
Repo for managing imports of external terminologies into MDB.

# 1. Uberon Ontology Integration Pipeline

## Overview

This project integrates Uberon anatomical ontology terms and their synonyms into the Model DataBase (MDB) graph.

Uberon is a multi-species anatomy ontology maintained by the Open Biological and Biomedical Ontologies (OBO) Foundry and widely used in biomedical datasets for standardized anatomical terminology.

Steps involved in this workflow are:

1. Extract relevant anatomical terms from Uberon
2. Convert ontology data into a format compatible for ingestion in the MDB data model
3. Insert Uberon terms into the graph as Term nodes
4. Insert synonym terms as additional Term nodes
5. Create Concept nodes** that link primary terms with their synonyms
6. Tag mappings to indicate their source ontology
7. Eventually connect these concepts to Value Set nodes so they can map to GC terms in the broader MDB ecosystem
8. Capture all executed Cypher queries so the ingestion process can be replayed on another MDB environment

This repository contains the full pipeline used to ingest Uberon anatomy terms into MDB.

---

## Workflow Overview

The ingestion pipeline follows these stages:
    Uberon OWL Ontology
        │
        ▼
    SPARQL Extraction
        │
        ▼
    CSV Ontology Dataset
        │
        ▼
    Flattening + Normalization
        │
        ▼
    JSON Flat Term Objects
        │
        ▼
    Python Ingestion Scripts
        │
        ▼
    Neo4j MDB Graph
        │
        ▼
    Term Nodes
    Concept Nodes
    Tag Nodes
        │
        ▼
    Future Integration
    Value Sets → GC Terms



Step 1 — Selecting the Uberon Dataset

The first step was identifying the appropriate Uberon dataset to meet MDB user requirements.

Uberon provides ontology releases in several formats:

- OWL
- OBO
- RDF
- JSON

For this pipeline we selected the latest OWL release for human view data because:

- It contains the full ontology structure for human anatomy (relevant to most regularly referenced terms in other data models)
- It preserves synonym relationships
- It allows flexible querying via SPARQL

Source: https://uberon.github.io/downloads.html#subsets


The OWL file contains:

- anatomical entities
- definitions
- cross references
- exact synonyms
- related synonyms
- ontology relationships

However, the OWL format is not directly usable for ingestion into MDB, so the ontology needed to be queried with sparql, extracted and transformed.

Step 2 — Extracting Data with SPARQL

To extract only the relevant ontology fields, SPARQL queries were written against the OWL file.

SPARQL was used to retrieve and store the following information into a csv file:

| Field | Description |
|------|-------------|
| id | Uberon identifier |
| label | primary term |
| definition | textual definition |
| exact synonyms | curated exact synonym list |
| related synonyms | additional synonyms |
| xrefs | external references |


---

Step 3 — Dataset Flattening

The CSV dataset contained nested synonym structures that are not compatible with the MDB format.

MDB requires:

- one Term node per textual value
- synonyms represented as separate terms
- synonym relationships represented through Concept nodes

The dataset was flattened into JSON records containing:

- flat main uberon term 
- flat synonym terms 
- synonym mappings

Example for mappings json:

```json
{
  "primary_value": "liver",
  "origin_id": "UBERON:0002107",
  "origin_name": "UBERON",
  "synonyms": [
    { "value": "hepatic organ" },
    { "value": "hepatic tissue" }
  ]
}

Step 4 - Inserting Terms

Primary Uberon terms were inserted into Neo4j as Term nodes using a Python ingestion script.

Term properties include:

| Property            | Description                   |
| ------------------- | ----------------------------- |
| value               | term text                     |
| handle              | normalized identifier         |
| origin_name         | ontology source               |
| origin_id           | ontology ID                   |
| origin_definition   | ontology definition           |
| external_references | ontology external reference   |
| nanoid              | unique identifier             |
| origin_version      | version number                |
| _commit             | ingestion commit              |


Step 5 — Inserting Synonym Terms

Synonyms were inserted as additional Term nodes.

This ensures:

-  synonym reuse across ontologies
-  normalized graph structure
-  easier semantic linking

Step 6 — Linking Terms with Concepts

Synonyms are linked to their primary terms through Concept nodes.

Structure:

(term) → represents → (concept) ← represents ← (term)
(concept) → has_tag → (tag) 

Concept nodes group synonymous terms while allowing additional metadata tags to be attached. Each concept created from Uberon mappings is associated with a Tag node indicating the mapping source.

Example:

mapping_src = "uberon"

This allows filtering or auditing ontology mappings by source.

Step 7. Value Set Integration 

(WIP)

## Repository Structure

Example project structure:

uberon_ingestion
├── data/
│ ├── human_view_clean.csv
│ ├── uberon_terms_main.yaml // converted from human_view_clean.csv using script "uberon_csv_to_yaml_json.py"
| ├── uberon_terms.json // converted from uberon_terms_main.yaml using script "uberon_csv_to_yaml_json.py"
│ ├── uberon_synonyms new.yaml // converted from human_view_clean.csv using script "uberon_csv_to_yaml_json.py"
│ ├── uberon_synonyms.json // converted from uberon_synonyms new.yaml using script "uberon_csv_to_yaml_json.py"
│ ├── uberon_synonym_mapping.json // mapping made to connect terms with synonyms, source: human_view_clean.csv, script: build_syn_mappings.py

│
├── scripts/
│ ├── build_syn_mappings.py //used to create the mapping file
│ ├── insert_uberon_relationships.py // used to merge relationships, create concept nodes and tag nodes
│ ├── insert_uberon_terms.py // used to merge term nodes and synonym nodes 
│ ├── test_connection.py // used to ensure connection 
│ ├── uberon_query.sparql // contains sparql query to extract data from .owl file 
│ ├── sanity_check.sparql // contains counting and tallying of data from .owl file 
│
├── logs/
│ └── cypher_queries.log (WIP)
│
└── README.md


### Directory Description

| Directory | Purpose |
|----------|---------|
| `data/` | Stores ontology source files and intermediate datasets produced during extraction and transformation. |
| `scripts/` | Contains Python scripts and SPARQL queries used for ontology extraction, transformation, and ingestion. |
| `logs/` | Stores logged Cypher queries executed during ingestion so the process can be replayed on another MDB instance. |
| `README.md` | Documentation describing the pipeline, workflow, and project structure. |