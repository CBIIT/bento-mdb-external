import json
import time
from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
USERNAME = ""          # <-- set this
PASSWORD = ""  # <-- set this
JSON_FILE = "uberon_synonyms.json"
BATCH_SIZE = 1000


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


def main():
    print("Script started", flush=True)

    # Load JSON
    print("Loading JSON file...", flush=True)
    try:
        with open(JSON_FILE, "r") as f:
            data = json.load(f)
        print(f"Loaded JSON with {len(data)} records", flush=True)
    except Exception as e:
        print("Failed loading JSON:", e, flush=True)
        return

    # Connect to Neo4j
    print("Connecting to Neo4j...", flush=True)
    driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

    try:
        driver.verify_connectivity()
        print("Connected to Neo4j successfully", flush=True)
    except Exception as e:
        print("Connectivity failed:", e, flush=True)
        driver.close()
        return

    print("Starting batch inserts...", flush=True)

    start_time = time.time()

    try:
        with driver.session() as session:
            for i in range(0, len(data), BATCH_SIZE):
                batch = data[i:i + BATCH_SIZE]
                batch_number = i // BATCH_SIZE + 1

                print(f"➡️ Batch {batch_number} | Records {i} - {i + len(batch)}", flush=True)

                try:
                    batch_start = time.time()
                    session.execute_write(insert_batch, batch)
                    batch_time = round(time.time() - batch_start, 2)

                    print(
                        f"Inserted {i + len(batch)} / {len(data)} "
                        f"(Batch took {batch_time}s)",
                        flush=True
                    )

                except Exception as e:
                    print(f" Error inserting batch starting at index {i}", flush=True)
                    print("Error:", e, flush=True)
                    break

    except Exception as outer_error:
        print("Session-level error:", outer_error, flush=True)

    driver.close()

    total_time = round(time.time() - start_time, 2)
    print(f"Done. Total time: {total_time}s", flush=True)


if __name__ == "__main__":
    main()