import csv
import json
import re

CSV_FILE = "human_view_clean.csv"
OUTPUT_FILE = "uberon_synonym_mapping.json"


def parse_synonym(entry):
    """
    Parses strings like:
    'anterior ramus [UBERON:3000155]'
    """

    entry = entry.strip()

    match = re.match(r"(.+?)\s*\[(.+?)\]$", entry)

    if not match:
        return {
            "raw": entry,
            "value": entry,
            "ref": None,
            "is_uberon": False
        }

    value = match.group(1).strip()
    ref = match.group(2).strip()

    return {
        "raw": entry,
        "value": value,
        "ref": ref,
        "is_uberon": ref.startswith("UBERON:")
    }


def main():

    mappings = []

    rows_seen = 0
    rows_written = 0

    with open(CSV_FILE, newline='', encoding="utf-8") as csvfile:

        reader = csv.DictReader(csvfile)

        print("CSV headers:", reader.fieldnames)

        for row in reader:

            rows_seen += 1

            primary = (row.get("label") or "").strip()

            if not primary:
                continue

            raw_exact = (row.get("exact_synonyms") or "").strip()

            if not raw_exact:
                continue

            synonyms = [s.strip() for s in raw_exact.split("|") if s.strip()]

            if not synonyms:
                continue

            parsed_synonyms = []
            has_uberon = False

            for s in synonyms:

                parsed = parse_synonym(s)

                if parsed["is_uberon"]:
                    has_uberon = True

                parsed_synonyms.append(parsed)

            mappings.append({
                "primary_term": primary,
                "synonyms": parsed_synonyms,
                "has_uberon_synonym": has_uberon
            })

            rows_written += 1

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(mappings, f, indent=2)

    print("\nFinished building mapping file")
    print("Rows read:", rows_seen)
    print("Mappings created:", rows_written)
    print("Output file:", OUTPUT_FILE)


if __name__ == "__main__":
    main()