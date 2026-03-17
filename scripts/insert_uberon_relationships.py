import json
import secrets
from datetime import datetime, timezone
from neo4j import GraphDatabase
import os

URI = "bolt://localhost:7687"
USERNAME = ""
PASSWORD = ""

JSON_FILE = "/Users/apple/Documents/FNL/Uberon Ingestion/data/uberon_synonym_mapping.json"
BATCH_SIZE = 200

DRY_RUN = True
LOG_FILE = "/Users/apple/Documents/FNL/Uberon Ingestion/logs/02_batch_insert_uberon_relationships.cql"


# ---------- Utility Generators ----------

def make_nanoid(size=6):
    alphabet = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKMNPQRSTUVWXYZ0123456789"
    return ''.join(secrets.choice(alphabet) for _ in range(size))


def make_commit():
    return f"CDEPV-{datetime.now(timezone.utc).strftime('%Y%m%d')}"


# ---------- Logging Helpers ----------

def init_log():
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)

    with open(LOG_FILE, "w") as f:
        f.write(f"// Cypher Migration File\n")
        f.write(f"// Generated: {datetime.now()}\n\n")
        f.write("BEGIN\n\n")


def finalize_log():
    with open(LOG_FILE, "a") as f:
        f.write("\nCOMMIT\n")


def format_value(value):
    if value is None:
        return "null"
    if isinstance(value, str):
        return '"' + value.replace('"', '\\"') + '"'
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list):
        return "[" + ", ".join(format_value(v) for v in value) + "]"
    if isinstance(value, dict):
        return "{ " + ", ".join(f"{k}: {format_value(v)}" for k, v in value.items()) + " }"
    return str(value)


def build_unwind_query(rows):
    formatted_rows = []

    for row in rows:
        formatted = "{ " + ", ".join(
            f"{k}: {format_value(v)}" for k, v in row.items()
        ) + " }"
        formatted_rows.append(formatted)

    unwind_block = "[\n  " + ",\n  ".join(formatted_rows) + "\n]"

    query = f"""
    UNWIND {unwind_block} AS row

    MATCH (primary:term {{
        value: row.primary_term,
        origin_name: "UBERON"
    }})

    WITH primary, row

    UNWIND row.synonyms AS syn

    OPTIONAL MATCH (s_ext:term {{external_references: syn.raw}})
    OPTIONAL MATCH (s_val:term {{value: syn.value}}) WHERE s_val <> primary

    WITH primary,
        row,
        collect(DISTINCT CASE
            WHEN syn.is_uberon = false THEN s_ext
            ELSE s_val
        END) AS syn_nodes

    WITH primary,
        row,
        [x IN syn_nodes WHERE x IS NOT NULL] AS valid_syns

    WHERE size(valid_syns) > 0

    MERGE (c:concept {{nanoid: row.concept_nanoid}})
    ON CREATE SET
        c._commit = row.commit

    MERGE (tag:tag {{nanoid: row.tag_nanoid}})
    ON CREATE SET
        tag.key = "mapping_src",
        tag.value = "UBERON"

    //  Relationships
    MERGE (c)-[:has_tag]->(tag)
    MERGE (primary)-[:represents]->(c)

    WITH c, valid_syns

    UNWIND valid_syns AS s

    MERGE (s)-[:represents]->(c)
"""
    return query


def log_query(query):
    with open(LOG_FILE, "a") as f:
        f.write("\n// -------- BATCH --------\n")
        f.write(query + "\n")


# ---------- DB Execution ----------

def insert_batch(tx, rows):
    query = """
    UNWIND $rows AS row

    MATCH (primary:term {
        value: row.primary_term,
        origin_name: "UBERON"
    })

    WITH primary, row

    UNWIND row.synonyms AS syn

    OPTIONAL MATCH (s_ext:term {external_references: syn.raw})
    OPTIONAL MATCH (s_val:term {value: syn.value}) WHERE s_val <> primary

    WITH primary,
         row,
         collect(DISTINCT CASE
             WHEN syn.is_uberon = false THEN s_ext
             ELSE s_val
         END) AS syn_nodes

    WITH primary,
         row,
         [x IN syn_nodes WHERE x IS NOT NULL] AS valid_syns

    WHERE size(valid_syns) > 0

    MERGE (c:concept {nanoid: row.concept_nanoid})
    ON CREATE SET
        c._commit = row.commit

    MERGE (tag:tag {nanoid: row.tag_nanoid})
    ON CREATE SET
        tag.key = "mapping_src",
        tag.value = "UBERON"

    MERGE (c)-[:has_tag]->(tag)
    MERGE (primary)-[:represents]->(c)

    WITH c, valid_syns

    UNWIND valid_syns AS s

    MERGE (s)-[:represents]->(c)
    """

    tx.run(query, rows=rows)


# ---------- MAIN ----------

def main():

    print("Starting relationship pipeline")

    if DRY_RUN:
        print("DRY RUN MODE: generating CQL only")
        init_log()

    driver = None
    if not DRY_RUN:
        driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))
        driver.verify_connectivity()

    with open(JSON_FILE, encoding="utf-8") as f:
        data = json.load(f)

    commit_id = make_commit()

    batch = []
    batch_number = 0
    processed = 0

    print("Commit:", commit_id)

    for item in data:

        processed += 1
        synonyms = item.get("synonyms", [])

        if not synonyms:
            continue

        batch.append({
            "concept_nanoid": make_nanoid(),
            "tag_nanoid": make_nanoid(),  
            "commit": commit_id,
            "primary_term": item["primary_term"],
            "synonyms": synonyms
        })

        if len(batch) >= BATCH_SIZE:

            batch_number += 1

            if DRY_RUN:
                query = build_unwind_query(batch)
                log_query(query)
            else:
                with driver.session() as session:
                    session.execute_write(insert_batch, batch)

            print(f"[Batch {batch_number}] Processed {len(batch)} rows")
            batch = []

    if batch:
        batch_number += 1

        if DRY_RUN:
            query = build_unwind_query(batch)
            log_query(query)
        else:
            with driver.session() as session:
                session.execute_write(insert_batch, batch)

        print(f"[Batch {batch_number}] Processed {len(batch)} rows")

    if driver:
        driver.close()

    if DRY_RUN:
        finalize_log()
        print(f"\nCQL written to: {LOG_FILE}")

    print("\nFinished.")
    print("Rows processed:", processed)


if __name__ == "__main__":
    main()