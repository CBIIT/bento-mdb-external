from neo4j import GraphDatabase

URI = ""
USERNAME = ""
PASSWORD = ""

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

try:
    driver.verify_connectivity()
    print(" Connected!")
    
    with driver.session() as session:
        result = session.run("RETURN 1 AS test")
        print("Query result:", result.single()["test"])

except Exception as e:
    print("Error:", e)

finally:
    driver.close()