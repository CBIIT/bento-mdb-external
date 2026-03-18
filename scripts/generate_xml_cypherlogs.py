import re
from datetime import datetime

INPUT_FILE = "/Users/apple/Documents/FNL/Uberon Ingestion/logs/03_cde_valueset_integration.cql"
OUTPUT_FILE = "/Users/apple/Documents/FNL/Uberon Ingestion/logs/cde_valueset_integration.xml"
AUTHOR = "Tazeen Shaukat"


def extract_batches(cql_text):
    """
    Extract Cypher queries between UNWIND ... COMMIT blocks
    """
    pattern = r"// -------- BATCH --------(.*?)COMMIT"
    matches = re.findall(pattern, cql_text, re.DOTALL)

    batches = []
    for m in matches:
        cleaned = m.strip()
        if cleaned:
            batches.append(cleaned)

    return batches


def generate_xml(batches):
    header = f"""<?xml version='1.0' encoding='UTF-8'?>
<databaseChangeLog 
    xmlns="http://www.liquibase.org/xml/ns/dbchangelog"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:neo4j="http://www.liquibase.org/xml/ns/dbchangelog-ext"
    xsi:schemaLocation="
        http://www.liquibase.org/xml/ns/dbchangelog 
        http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-latest.xsd">

"""

    body = ""

    for i, batch in enumerate(batches, start=1):
        body += f"""  <changeSet id="{i}" author="{AUTHOR}">
    <neo4j:cypher><![CDATA[
{batch}
    ]]></neo4j:cypher>
  </changeSet>

"""

    footer = "</databaseChangeLog>"

    return header + body + footer


def main():
    with open(INPUT_FILE, "r") as f:
        cql_text = f.read()

    batches = extract_batches(cql_text)

    xml_content = generate_xml(batches)

    with open(OUTPUT_FILE, "w") as f:
        f.write(xml_content)

    print(f"Generated {OUTPUT_FILE} with {len(batches)} changeSets")


if __name__ == "__main__":
    main()