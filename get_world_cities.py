import os
import json
import time
import requests

# --------- Config ---------
DATA_DIR = "data"
INPUT_FILE = os.path.join(DATA_DIR, "cities_input.json")
OUTPUT_FILE = os.path.join(DATA_DIR, "cities_enriched.jsonl")
QID_CACHE_FILE = os.path.join(DATA_DIR, "qid_lookup.json")
COUNTRIES_FILE = os.path.join(DATA_DIR, "countries_cache.json")
STATES_FILE = os.path.join(DATA_DIR, "states_cache.json")

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
WIKIDATA_ENTITY_API = "https://www.wikidata.org/wiki/Special:EntityData/{}.json"

# --------- Helpers ---------
def load_json(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_json(file_path, data):
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def append_jsonl(file_path, record):
    with open(file_path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(record, ensure_ascii=False) + '\n')

def load_processed_city_names():
    processed = set()
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    record = json.loads(line.strip())
                    processed.add(record.get("CityNameEn"))
                except json.JSONDecodeError:
                    continue
    return processed

# --------- Wikidata Integration ---------
def get_wikidata_id(name, qid_lookup):
    if name in qid_lookup:
        return qid_lookup[name]
    params = {
        "action": "wbsearchentities",
        "format": "json",
        "language": "en",
        "search": name,
        "type": "item",
        "limit": 1
    }
    try:
        response = requests.get(WIKIDATA_API, params=params, timeout=10)
        data = response.json()
        if data.get("search"):
            qid = data["search"][0]["id"]
            qid_lookup[name] = qid
            save_json(QID_CACHE_FILE, qid_lookup)
            return qid
    except Exception as e:
        print(f"Error finding QID for {name}: {e}")
    return None

def get_entity_info(qid, level):
    try:
        response = requests.get(WIKIDATA_ENTITY_API.format(qid), timeout=10)
        data = response.json()
        entity = data.get("entities", {}).get(qid, {})
        labels = entity.get("labels", {})
        claims = entity.get("claims", {})

        def get_claim(prop):
            values = claims.get(prop, [])
            if not values:
                return None
            return values[0].get("mainsnak", {}).get("datavalue", {}).get("value")

        info = {
            f"{level}QID": qid,
            f"{level}NameEn": labels.get("en", {}).get("value"),
            f"{level}NameAr": labels.get("ar", {}).get("value")
        }

        if level == "Country":
            info["IsoAlpha2"] = get_claim("P297")
            capital = get_claim("P36")
            if isinstance(capital, dict):
                info["CapitalQID"] = capital.get("id")

        if level == "State":
            info["IsoCode"] = get_claim("P300")
            parent = get_claim("P17")
            if isinstance(parent, dict):
                info["CountryQID"] = parent.get("id")

        return info
    except Exception as e:
        print(f"Error loading entity {qid} ({level}): {e}")
        return {}

def enrich_city(qid):
    try:
        response = requests.get(WIKIDATA_ENTITY_API.format(qid), timeout=10)
        data = response.json()
        entity = data.get("entities", {}).get(qid, {})
        labels = entity.get("labels", {})
        claims = entity.get("claims", {})

        def get_claim(prop):
            values = claims.get(prop, [])
            if not values:
                return None
            return values[0].get("mainsnak", {}).get("datavalue", {}).get("value")

        return {
            "CityNameAr": labels.get("ar", {}).get("value"),
            "CountryQID": get_claim("P17").get("id") if isinstance(get_claim("P17"), dict) else None,
            "StateQID": get_claim("P131").get("id") if isinstance(get_claim("P131"), dict) else None,
            "Latitude": get_claim("P625")["latitude"] if get_claim("P625") else None,
            "Longitude": get_claim("P625")["longitude"] if get_claim("P625") else None,
            "Population": get_claim("P1082").get("amount") if isinstance(get_claim("P1082"), dict) else get_claim("P1082"),
            "IsoCode": get_claim("P300")
        }
    except Exception as e:
        print(f"Error enriching city QID {qid}: {e}")
        return {}

# --------- Main Process ---------
def process_entries():
    os.makedirs(DATA_DIR, exist_ok=True)

    cities = json.load(open(INPUT_FILE, 'r', encoding='utf-8'))
    qid_lookup = load_json(QID_CACHE_FILE)
    country_cache = load_json(COUNTRIES_FILE)
    state_cache = load_json(STATES_FILE)
    processed = load_processed_city_names()

    to_process = [c for c in cities if c["CityNameEn"] not in processed]
    print(f"Processing {len(to_process)} cities...")

    for idx, city in enumerate(to_process, 1):
        city_name = city["CityNameEn"]
        print(f"[{idx}/{len(to_process)}] {city_name}")

        qid = get_wikidata_id(city_name, qid_lookup)
        if not qid:
            continue

        city_info = enrich_city(qid)
        city.update(city_info)

        # Country enrichment
        cqid = city_info.get("CountryQID")
        if cqid:
            if cqid not in country_cache:
                country_cache[cqid] = get_entity_info(cqid, "Country")
                save_json(COUNTRIES_FILE, country_cache)
            country_info = country_cache[cqid]
            city["CountryDetails"] = country_info

            # Capital check
            capital_qid = country_info.get("CapitalQID")
            city["IsCapital"] = capital_qid == qid
        else:
            city["IsCapital"] = False

        # State enrichment
        sqid = city_info.get("StateQID")
        if sqid:
            if sqid not in state_cache:
                state_cache[sqid] = get_entity_info(sqid, "State")
                save_json(STATES_FILE, state_cache)
            city["StateDetails"] = state_cache[sqid]

        append_jsonl(OUTPUT_FILE, city)
        time.sleep(1)

# --------- Run ---------
if __name__ == "__main__":
    process_entries()
