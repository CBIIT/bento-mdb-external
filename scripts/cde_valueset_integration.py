from neo4j import GraphDatabase
import secrets
from datetime import datetime, timezone
import os

URI = "bolt://localhost:7687"
USERNAME = ""
PASSWORD = ""

DRY_RUN = True
LOG_FILE = "/Users/apple/Documents/FNL/Uberon Ingestion/logs/03_cde_valueset_integration.cql"


# ---------- Generators ----------

def make_nanoid(size=6):
    alphabet = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKMNPQRSTUVWXYZ0123456789"
    return ''.join(secrets.choice(alphabet) for _ in range(size))


def make_commit():
    return f"test-Uberon-{datetime.now(timezone.utc).strftime('%Y%m%d')}"


# ---------- CDE CONFIG ----------

CDE_LIST = [
{
"cde_handle":"disease_primary_site_of_disease_uberon_identifier",
"origin_id":"14883047",
"property_handle":"primary_site",
"property_desc":"Anatomical site of disease in primary diagnosis for this diagnosis in Uberon terminology"
},
{
"cde_handle":"disease_tissue_or_organ_of_origin_anatomic_site_uberon_identifier",
"origin_id":"14883058",
"property_handle":"anatomic_site",
"property_desc":"Anatomical site of disease, tissue or organ of disease origin in this diagnosis in Uberon terminology"
},
{
"cde_handle":"disease_site_of_resection_or_biopsy_uberon_identifier",
"origin_id":"14883054",
"property_handle":"site_of_resection",
"property_desc":"Site of resection or biopsy done for this diagnosis in Uberon terminology"
},
{
"cde_handle":"specimen_original_anatomic_site_uberon_identifier",
"origin_id":"12083894",
"property_handle":"specimen_anatomic_site_id",
"property_desc":"Specimen original anatomic site ID from Uberon"
},
{
"cde_handle":"specimen_original_anatomic_site_uberon_name",
"origin_id":"12299653",
"property_handle":"specimen_anatomic_site_name",
"property_desc":"Specimen original anatomic site name from Uberon"
}
]


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
    if isinstance(value, list):
        return "[" + ", ".join(format_value(v) for v in value) + "]"
    return str(value)


def build_unwind_query(rows, valueset_nanoid, commit):

    formatted_rows = []

    for row in rows:
        formatted = "{ " + ", ".join(
            f"{k}: {format_value(v)}" for k, v in row.items()
        ) + " }"
        formatted_rows.append(formatted)

    unwind_block = "[\n  " + ",\n  ".join(formatted_rows) + "\n]"

    query = f"""
    // ---------- CREATE VALUESET ----------
    MERGE (vs:value_set {{nanoid: "{valueset_nanoid}"}})
    ON CREATE SET
        vs._commit = "{commit}",
        vs.handle = "uberon_terms_valueset"

    WITH vs

    MATCH (t:term {{origin_name:"UBERON"}})
    MERGE (vs)-[:has_term]->(t)

    // ---------- CDE MAPPINGS ----------
    UNWIND {unwind_block} AS row

    MATCH (cde:term {{
        handle: row.cde_handle,
        origin_id: row.origin_id,
        origin_name: "caDSR",
        origin_version: "1.00"
    }})

    MERGE (p:property {{nanoid: row.property_nanoid}})
    ON CREATE SET
        p._commit = row.commit,
        p.handle = row.property_handle,
        p.desc = row.property_desc,
        p.is_key = false,
        p.is_nullable = false,
        p.is_required = false,
        p.is_strict = true,
        p.model = "UBERON",
        p.value_domain = "value_set",
        p.version = "2025-12-04"

    MERGE (c:concept {{nanoid: row.concept_nanoid}})
    ON CREATE SET
        c._commit = row.commit

    MERGE (tag:tag {{nanoid: row.tag_nanoid}})
    ON CREATE SET
        tag.key = "mapping_src",
        tag.value = "UBERON",
        tag._commit = row.commit

    MERGE (c)-[:has_tag]->(tag)
    MERGE (p)-[:has_concept]->(c)
    MERGE (cde)-[:represents]->(c)
    MERGE (p)-[:has_value_set]->(vs)
    """
    return query


def log_query(query):
    with open(LOG_FILE, "a") as f:
        f.write("\n// -------- BATCH --------\n")
        f.write(query + "\n")


# ---------- DB Execution ----------

def execute_batch(tx, rows, valueset_nanoid, commit):

    query = """
    MERGE (vs:value_set {nanoid: $valueset_nanoid})
    ON CREATE SET
        vs._commit = $commit,
        vs.handle = "uberon_terms_valueset"

    WITH vs

    MATCH (t:term {origin_name:"UBERON"})
    MERGE (vs)-[:has_term]->(t)

    WITH vs

    UNWIND $rows AS row

    MATCH (cde:term {
        handle: row.cde_handle,
        origin_id: row.origin_id,
        origin_name: "caDSR",
        origin_version: "1.00"
    })

    MERGE (p:property {nanoid: row.property_nanoid})
    ON CREATE SET
        p._commit = row.commit,
        p.handle = row.property_handle,
        p.desc = row.property_desc,
        p.is_key = false,
        p.is_nullable = false,
        p.is_required = false,
        p.is_strict = true,
        p.model = "UBERON",
        p.value_domain = "value_set",
        p.version = "2025-12-04"

    MERGE (c:concept {nanoid: row.concept_nanoid})
    ON CREATE SET
        c._commit = row.commit

    MERGE (tag:tag {nanoid: row.tag_nanoid})
    ON CREATE SET
        tag.key = "mapping_src",
        tag.value = "UBERON",
        tag._commit = row.commit

    MERGE (c)-[:has_tag]->(tag)
    MERGE (p)-[:has_concept]->(c)
    MERGE (cde)-[:represents]->(c)
    MERGE (p)-[:has_value_set]->(vs)
    """

    tx.run(query, rows=rows, valueset_nanoid=valueset_nanoid, commit=commit)


# ---------- MAIN ----------

def main():

    print("Starting CDE ValueSet pipeline")

    if DRY_RUN:
        print("DRY RUN MODE")
        init_log()

    driver = None
    if not DRY_RUN:
        driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))
        driver.verify_connectivity()

    commit = make_commit()
    valueset_nanoid = make_nanoid()

    print("Commit:", commit)
    print("ValueSet nanoid:", valueset_nanoid)

    rows = []

    for config in CDE_LIST:
        rows.append({
            "cde_handle": config["cde_handle"],
            "origin_id": config["origin_id"],
            "property_handle": config["property_handle"],
            "property_desc": config["property_desc"],
            "property_nanoid": make_nanoid(),
            "concept_nanoid": make_nanoid(),
            "tag_nanoid": make_nanoid(),
            "commit": commit
        })

    if DRY_RUN:
        query = build_unwind_query(rows, valueset_nanoid, commit)
        log_query(query)
    else:
        with driver.session() as session:
            session.execute_write(execute_batch, rows, valueset_nanoid, commit)

    if driver:
        driver.close()

    if DRY_RUN:
        finalize_log()
        print(f"CQL written to {LOG_FILE}")

    print("Finished.")


if __name__ == "__main__":
    main()