import snowflake.connector
import os
import random
from dotenv import load_dotenv

load_dotenv()

# ─── CALIFORNIA RESTAURANT DATA ───────────────────────────────────────────────

CITIES = [
    {"name": "San Francisco", "zip_codes": ["94102","94103","94105","94107","94110"],"lat_range": (37.70, 37.81),"lng_range": (-122.51, -122.38)},
    {"name": "Los Angeles",   "zip_codes": ["90001","90028","90036","90046","90210"],"lat_range": (33.90, 34.15),"lng_range": (-118.45, -118.15)},
    {"name": "San Diego",     "zip_codes": ["92101","92103","92107","92108","92115"],"lat_range": (32.65, 32.80),"lng_range": (-117.25, -117.05)},
    {"name": "Sacramento",    "zip_codes": ["95814","95816","95818","95820","95822"],"lat_range": (38.52, 38.65),"lng_range": (-121.55, -121.42)},
    {"name": "Oakland",       "zip_codes": ["94601","94602","94606","94609","94612"],"lat_range": (37.75, 37.87),"lng_range": (-122.28, -122.16)},
    {"name": "San Jose",      "zip_codes": ["95101","95110","95112","95116","95128"],"lat_range": (37.28, 37.40),"lng_range": (-122.00, -121.82)},
    {"name": "Santa Monica",  "zip_codes": ["90401","90402","90403","90404","90405"],"lat_range": (33.99, 34.05),"lng_range": (-118.52, -118.46)},
    {"name": "Pasadena",      "zip_codes": ["91101","91103","91104","91105","91106"],"lat_range": (34.12, 34.20),"lng_range": (-118.18, -118.07)},
    {"name": "Berkeley",      "zip_codes": ["94702","94703","94704","94705","94710"],"lat_range": (37.84, 37.90),"lng_range": (-122.30, -122.23)},
    {"name": "Irvine",        "zip_codes": ["92602","92603","92604","92612","92618"],"lat_range": (33.63, 33.74),"lng_range": (-117.87, -117.74)},
]

CUISINES = [
    "Italian", "Japanese", "Mexican", "Indian", "Chinese",
    "Thai", "Mediterranean", "American", "Vegan", "Seafood",
    "Korean", "French", "Greek", "Vietnamese", "Ethiopian"
]

RESTAURANT_NAME_TEMPLATES = {
    "Italian":       ["Osteria {}", "Trattoria {}", "La {} Cucina", "Ristorante {}", "Caffe {}"],
    "Japanese":      ["{} Sushi", "Sakura {}", "{} Ramen House", "Nori {}", "{} Izakaya"],
    "Mexican":       ["Casa {}", "El {} Cantina", "Taqueria {}", "La {} Cocina", "Hacienda {}"],
    "Indian":        ["{} Spice", "Taj {}", "{} Palace", "Curry {} House", "Masala {}"],
    "Chinese":       ["{} Garden", "Golden {} Palace", "{} Dynasty", "Jade {}", "Lucky {} Kitchen"],
    "Thai":          ["{} Thai Kitchen", "Bangkok {}", "Lotus {}", "{} Orchid", "Thai {} House"],
    "Mediterranean": ["{} Mezze", "Olive {}", "The {} Table", "Aegean {}", "{} Bistro"],
    "American":      ["The {} Grill", "{} Kitchen", "Smoky {}", "The {} Diner", "{} Burger Co"],
    "Vegan":         ["Green {}", "The {} Leaf", "Plant {} Kitchen", "Roots {}", "{} Garden Cafe"],
    "Seafood":       ["The {} Catch", "Ocean {} Grill", "{} Fish House", "Blue {} Seafood", "Harbor {}"],
    "Korean":        ["Seoul {}", "{} BBQ House", "Gangnam {}", "{} Tofu House", "Hanok {}"],
    "French":        ["Le {} Bistro", "Café {}", "Maison {}", "La {} Brasserie", "Chez {}"],
    "Greek":         ["The {} Taverna", "Athena {}", "Olympia {} Grill", "Santorini {}", "Zeus {}"],
    "Vietnamese":    ["Pho {} House", "Saigon {}", "{} Noodle Bar", "Hanoi {}", "Mekong {}"],
    "Ethiopian":     ["{} Injera House", "Addis {}", "Blue {} Ethiopian", "Habesha {}", "{} Cafe"],
}

NAME_WORDS = [
    "Rosa", "Luna", "Marco", "Bella", "Verde", "Coral", "Mango", "River",
    "Golden", "Silver", "Pacific", "Bay", "Sunset", "Harbor", "Vine",
    "Garden", "Stone", "Oak", "Cedar", "Palm", "Canyon", "Valley", "Peak"
]

STREET_NAMES = [
    "Main St", "Oak Ave", "Maple Blvd", "Pacific Ave", "Mission St",
    "Market St", "Broadway", "Valencia St", "Sunset Blvd", "Ocean Ave",
    "Castro St", "Grand Ave", "Union St", "Columbus Ave", "Harbor Blvd"
]

PRICE_RANGES = ["$", "$$", "$$$", "$$$$"]
NOISE_LEVELS = ["Quiet", "Moderate", "Lively"]
VIBES = ["Casual", "Fine Dining", "Trendy", "Cozy", "Outdoor", "Quick Bite"]

def get_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

def generate_restaurant(city, cuisine, index):
    word = random.choice(NAME_WORDS)
    template = random.choice(RESTAURANT_NAME_TEMPLATES[cuisine])
    name = template.format(word)

    street_num = random.randint(100, 9999)
    street = random.choice(STREET_NAMES)
    address = f"{street_num} {street}"
    zip_code = random.choice(city["zip_codes"])

    lat = round(random.uniform(*city["lat_range"]), 6)
    lng = round(random.uniform(*city["lng_range"]), 6)

    price = random.choices(PRICE_RANGES, weights=[20, 40, 30, 10])[0]
    rating = round(random.uniform(3.5, 5.0), 1)
    noise = random.choice(NOISE_LEVELS)
    outdoor = random.choice([True, False])
    parking = random.choice([True, False])
    reservations = random.choice([True, True, False])

    restaurant_id = f"REST_SYN_{city['name'][:3].upper()}_{cuisine[:3].upper()}_{index:04d}"
    synthetic_id = f"syn_{city['name'][:3].lower()}_{cuisine[:3].lower()}_{index:04d}"

    return (
        restaurant_id, name, cuisine, price, rating,
        address, city["name"], zip_code, lat, lng, noise,
        outdoor, parking, reservations, synthetic_id
    )

def main():
    print("🔌 Connecting to Snowflake...")
    conn = get_conn()
    cursor = conn.cursor()

    total = 0
    index = 1

    for city in CITIES:
        for cuisine in CUISINES:
            count = random.randint(4, 6)
            for _ in range(count):
                restaurant = generate_restaurant(city, cuisine, index)
                try:
                    cursor.execute("""
                        INSERT INTO REMY_DB.CORE.RESTAURANTS
                        (restaurant_id, name, cuisine_type, price_range, avg_rating,
                         address, city, zip_code, lat, lng, noise_level,
                         has_outdoor_seating, has_parking, accepts_reservations, yelp_id)
                        SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                        WHERE NOT EXISTS (
                            SELECT 1 FROM REMY_DB.CORE.RESTAURANTS WHERE yelp_id = %s
                        )
                    """, (*restaurant, restaurant[-1]))
                    total += 1
                except Exception as e:
                    print(f"  ⚠️  Skipped: {e}")
                index += 1

        print(f"✅ {city['name']} done")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"\n🎉 Done! {total} restaurants seeded across California.")

if __name__ == "__main__":
    main()