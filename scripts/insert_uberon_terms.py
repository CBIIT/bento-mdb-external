import json
import time
from neo4j import GraphDatabase
from datetime import datetime
import os

URI = "bolt://localhost:7687"
USERNAME = ""
PASSWORD = ""

JSON_FILE = "/Users/apple/Documents/FNL/Uberon Ingestion/data/uberon_terms.json"
BATCH_SIZE = 1000

# NEW FLAGS
DRY_RUN = True # switch to false if inserting data, true if generating logs  
LOG_FILE = "/Users/apple/Documents/FNL/Uberon Ingestion/logs/01_batch_insert_uberon_terms.cql"


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
    return str(value)


def build_unwind_query(rows):
    unwind_rows = []

    for row in rows:
        formatted = "{ " + ", ".join(
            f"{k}: {format_value(v)}" for k, v in row.items()
        ) + " }"
        unwind_rows.append(formatted)

    unwind_block = "[\n  " + ",\n  ".join(unwind_rows) + "\n]"

    query = f"""
UNWIND {unwind_block} AS row
MERGE (t:term {{
    origin_name: row.origin_name,
    origin_id: row.origin_id
}})
SET t._commit = row._commit,
    t.handle = row.handle,
    t.nanoid = row.nanoid,
    t.origin_version = row.origin_version,
    t.origin_definition = row.origin_definition,
    t.value = row.value,
    t.external_references = row.external_references
"""

    return query


def log_query(query):
    with open(LOG_FILE, "a") as f:
        f.write("\n// -------- BATCH --------\n")
        f.write(query + "\n")


# ---------- DB Function ----------

def insert_batch(tx, rows):
    query = """
    UNWIND $rows AS row
    MERGE (t:term {
        origin_name: row.origin_name,
        origin_id: row.origin_id
    })
    SET t._commit = row._commit,
        t.handle = row.handle,
        t.nanoid = row.nanoid,
        t.origin_version = row.origin_version,
        t.origin_definition = row.origin_definition,
        t.value = row.value,
        t.external_references = row.external_references
    """

    tx.run(query, rows=rows)


# ---------- MAIN ----------

def main():
    print("Script started", flush=True)

    init_log()

    # Load JSON
    print("Loading JSON file...", flush=True)
    try:
        with open(JSON_FILE, "r") as f:
            data = json.load(f)
        print(f"Loaded JSON with {len(data)} records", flush=True)
    except Exception as e:
        print("Failed loading JSON:", e, flush=True)
        return

    driver = None

    if not DRY_RUN:
        print("Connecting to Neo4j...", flush=True)
        driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

        try:
            driver.verify_connectivity()
            print("Connected to Neo4j successfully", flush=True)
        except Exception as e:
            print("Connectivity failed:", e, flush=True)
            driver.close()
            return

    print("Starting batch processing...", flush=True)

    start_time = time.time()

    try:
        if not DRY_RUN:
            session_ctx = driver.session()
        else:
            session_ctx = None

        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i:i + BATCH_SIZE]
            batch_number = i // BATCH_SIZE + 1

            print(f"➡️ Batch {batch_number} | Records {i} - {i + len(batch)}", flush=True)

            # Build full Cypher
            cypher_query = build_unwind_query(batch)
            log_query(cypher_query)

            if not DRY_RUN:
                try:
                    batch_start = time.time()
                    session_ctx.execute_write(insert_batch, batch)
                    batch_time = round(time.time() - batch_start, 2)

                    print(
                        f"Inserted {i + len(batch)} / {len(data)} "
                        f"(Batch took {batch_time}s)",
                        flush=True
                    )

                except Exception as e:
                    print(f"Error inserting batch starting at index {i}", flush=True)
                    print("Error:", e, flush=True)
                    break

    except Exception as outer_error:
        print("Session-level error:", outer_error, flush=True)

    if driver:
        driver.close()

    finalize_log()

    total_time = round(time.time() - start_time, 2)
    print(f"Done. Total time: {total_time}s", flush=True)
    print(f"Cypher log written to: {LOG_FILE}", flush=True)


if __name__ == "__main__":
    main()