from neo4j import GraphDatabase
import secrets
from datetime import datetime, timezone


URI = "bolt://localhost:7687"
USERNAME = ""
PASSWORD = ""


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


# ---------- MAIN INGEST FUNCTION ----------

def process_cde(tx, config, commit):

    property_nanoid = make_nanoid()
    concept_nanoid = make_nanoid()
    valueset_nanoid = make_nanoid()
    tag_nanoid = make_nanoid()

    params = {
        "cde_handle": config["cde_handle"],
        "origin_id": config["origin_id"],
        "property_handle": config["property_handle"],
        "property_desc": config["property_desc"],
        "property_nanoid": property_nanoid,
        "concept_nanoid": concept_nanoid,
        "valueset_nanoid": valueset_nanoid,
        "tag_nanoid": tag_nanoid,
        "commit": commit
    }

    query = """
    MATCH (cde:term {
        handle:$cde_handle,
        origin_id:$origin_id,
        origin_name:"caDSR",
        origin_version:"1.00"
    })

    CREATE (p:property {
        _commit:$commit,
        nanoid:$property_nanoid,
        handle:$property_handle,
        desc:$property_desc,
        is_key:false,
        is_nullable:false,
        is_required:false,
        is_strict:true,
        model:"UBERON",
        value_domain:"value_set",
        version:"2025-12-04"
    })

    CREATE (c:concept {
        nanoid:$concept_nanoid,
        _commit:$commit
    })

    CREATE (tag:tag {
        nanoid:$tag_nanoid,
        key:"mapping_src",
        value:"UBERON"
    })

    CREATE (c)-[:has_tag]->(tag)

    CREATE (p)-[:has_concept]->(c)

    CREATE (cde)-[:represents]->(c)

    CREATE (vs:value_set {
        nanoid:$valueset_nanoid,
        _commit:$commit
    })

    CREATE (p)-[:has_value_set]->(vs)

    WITH vs
    MATCH (t:term {origin_name:"UBERON"})
    CREATE (vs)-[:has_term]->(t)
    """

    print("\nExecuting mapping for:", config["cde_handle"])
    print("property nanoid:", property_nanoid)
    print("concept nanoid:", concept_nanoid)
    print("valueset nanoid:", valueset_nanoid)
    print("tag nanoid:", tag_nanoid)

    tx.run(query, params)


# ---------- MAIN ----------

def main():

    commit = make_commit()
    print("Commit ID:", commit)

    driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

    with driver.session() as session:

        for config in CDE_LIST:
            session.execute_write(process_cde, config, commit)

    driver.close()


if __name__ == "__main__":
    main()