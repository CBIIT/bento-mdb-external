import json
import secrets
from datetime import datetime, timezone
from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
USERNAME = ""
PASSWORD = ""

JSON_FILE = "uberon_synonym_mapping.json"
BATCH_SIZE = 200


# ---------- Utility Generators ----------

def make_nanoid(size=6):
    alphabet = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKMNPQRSTUVWXYZ0123456789"
    return ''.join(secrets.choice(alphabet) for _ in range(size))


def make_commit():
    return f"CDEPV-{datetime.now(timezone.utc).strftime('%Y%m%d')}"


# ---------- Cypher Insert ----------

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

    CREATE (c:concept {
        nanoid: row.concept_nanoid,
        _commit: row.commit
    })

    CREATE (tag:tag {
        nanoid: row.tag_nanoid,
        key: "mapping_src",
        value: "UBERON"
    })

    MERGE (c)-[:has_tag]->(tag)

    MERGE (primary)-[:represents]->(c)

    WITH c, valid_syns

    UNWIND valid_syns AS s

    MERGE (s)-[:represents]->(c)
    """

    tx.run(query, rows=rows)


# ---------- Main ----------

def main():

    driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

    with open(JSON_FILE, encoding="utf-8") as f:
        data = json.load(f)

    commit_id = make_commit()

    batch = []
    batch_number = 0
    inserted = 0
    processed = 0

    print("Starting relationship creation")
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

            with driver.session() as session:
                session.execute_write(insert_batch, batch)

            inserted += len(batch)

            print(
                f"[Batch {batch_number}] "
                f"Rows inserted: {len(batch)} | "
                f"Total: {inserted} | "
                f"Processed: {processed}"
            )

            batch = []

    if batch:

        batch_number += 1

        with driver.session() as session:
            session.execute_write(insert_batch, batch)

        inserted += len(batch)

    driver.close()

    print("\nFinished.")
    print("Rows processed:", processed)
    print("Concept rows attempted:", inserted)


if __name__ == "__main__":
    main()