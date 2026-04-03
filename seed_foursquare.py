import requests
import snowflake.connector
import os
import time
from dotenv import load_dotenv

load_dotenv()

# ─── CONFIG ───────────────────────────────────────────────────────────────────
FSQ_API_KEY = os.getenv("FOURSQUARE_API_KEY")
FSQ_URL = "https://api.foursquare.com/v3/places/search"

HEADERS = {
    "Accept": "application/json",
    "Authorization": FSQ_API_KEY
}

# California cities with coordinates
CITIES = [
    {"name": "San Francisco", "lat": 37.7749, "lng": -122.4194},
    {"name": "Los Angeles",   "lat": 34.0522, "lng": -118.2437},
    {"name": "San Diego",     "lat": 32.7157, "lng": -117.1611},
    {"name": "Sacramento",    "lat": 38.5816, "lng": -121.4944},
    {"name": "Oakland",       "lat": 37.8044, "lng": -122.2712},
    {"name": "San Jose",      "lat": 37.3382, "lng": -121.8863},
    {"name": "Santa Monica",  "lat": 34.0195, "lng": -118.4912},
    {"name": "Pasadena",      "lat": 34.1478, "lng": -118.1445},
    {"name": "Berkeley",      "lat": 37.8716, "lng": -122.2727},
    {"name": "Irvine",        "lat": 33.6846, "lng": -117.8265},
]

# Cuisine categories (Foursquare category IDs for restaurants)
CUISINES = [
    {"name": "Italian",       "query": "italian restaurant"},
    {"name": "Japanese",      "query": "japanese restaurant"},
    {"name": "Mexican",       "query": "mexican restaurant"},
    {"name": "Indian",        "query": "indian restaurant"},
    {"name": "Chinese",       "query": "chinese restaurant"},
    {"name": "Thai",          "query": "thai restaurant"},
    {"name": "Mediterranean", "query": "mediterranean restaurant"},
    {"name": "American",      "query": "american restaurant"},
    {"name": "Vegan",         "query": "vegan restaurant"},
    {"name": "Seafood",       "query": "seafood restaurant"},
]

# ─── PRICE MAPPING ────────────────────────────────────────────────────────────
def map_price(level):
    mapping = {1: "$", 2: "$$", 3: "$$$", 4: "$$$$"}
    return mapping.get(level, "$$")

# ─── SNOWFLAKE CONNECTION ─────────────────────────────────────────────────────
def get_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

# ─── INSERT INTO SNOWFLAKE ────────────────────────────────────────────────────
def insert_restaurant(cursor, place, city_name, cuisine_name):
    fsq_id = place.get("fsq_id", "")
    restaurant_id = "REST_FSQ_" + fsq_id[:15].upper()
    name = place.get("name", "Unknown")
    price = map_price(place.get("price", 2))
    rating = round(place.get("rating", 7.0) / 2, 1)  # Foursquare rates 0-10, convert to 0-5
    address = place.get("location", {}).get("formatted_address", "")
    zip_code = place.get("location", {}).get("postcode", "")
    lat = place.get("geocodes", {}).get("main", {}).get("latitude", 0)
    lng = place.get("geocodes", {}).get("main", {}).get("longitude", 0)

    try:
        cursor.execute("""
            INSERT INTO REMY_DB.CORE.RESTAURANTS
            (restaurant_id, name, cuisine_type, price_range, avg_rating,
             address, city, zip_code, lat, lng, noise_level,
             has_outdoor_seating, has_parking, accepts_reservations, yelp_id)
            SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM REMY_DB.CORE.RESTAURANTS WHERE yelp_id = %s
            )
        """, (
            restaurant_id, name, cuisine_name, price, rating,
            address, city_name, zip_code, lat, lng, "Moderate",
            False, False, True, fsq_id,
            fsq_id
        ))
    except Exception as e:
        print(f"  ⚠️  Skipped {name}: {e}")

# ─── FETCH FROM FOURSQUARE ────────────────────────────────────────────────────
def fetch_restaurants(city, cuisine):
    params = {
        "query": cuisine["query"],
        "ll": f"{city['lat']},{city['lng']}",
        "radius": 5000,
        "limit": 50,
        "fields": "fsq_id,name,location,price,rating,geocodes"
    }
    try:
        response = requests.get(FSQ_URL, headers=HEADERS, params=params)
        if response.status_code != 200:
            print(f"  ⚠️  Foursquare error: {response.status_code} — {response.text}")
            return []
        return response.json().get("results", [])
    except Exception as e:
        print(f"  ⚠️  Request error: {e}")
        return []

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print("🔌 Connecting to Snowflake...")
    conn = get_conn()
    cursor = conn.cursor()
    total = 0

    for city in CITIES:
        for cuisine in CUISINES:
            print(f"📍 Fetching {cuisine['name']} restaurants in {city['name']}...")
            places = fetch_restaurants(city, cuisine)

            for place in places:
                insert_restaurant(cursor, place, city["name"], cuisine["name"])
                total += 1

            print(f"   ✅ {len(places)} restaurants processed")
            time.sleep(0.5)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"\n🎉 Done! {total} restaurants seeded into Snowflake.")

if __name__ == "__main__":
    main()