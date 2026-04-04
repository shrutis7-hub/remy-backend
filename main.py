from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import snowflake.connector
from groq import Groq
from dotenv import load_dotenv
import os

load_dotenv()
app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

# --- ENDPOINT 1: Get all restaurants ---
@app.get("/restaurants")
def get_restaurants():
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT restaurant_id, name, cuisine_type, price_range, avg_rating FROM REMY_DB.CORE.RESTAURANTS")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [
        {
            "restaurant_id": r[0],
            "name": r[1],
            "cuisine_type": r[2],
            "price_range": r[3],
            "avg_rating": r[4]
        } for r in rows
    ]

# --- ENDPOINT 2: Get recommendations for a session ---
@app.get("/recommendations/{session_id}")
def get_recommendations(session_id: str):
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT r.name, r.cuisine_type, r.price_range, r.avg_rating,
               ai.match_score, ai.match_reason, ai.rank
        FROM REMY_DB.CORE.AI_RECOMMENDATIONS ai
        JOIN REMY_DB.CORE.RESTAURANTS r ON ai.restaurant_id = r.restaurant_id
        WHERE ai.session_id = %s
        ORDER BY ai.rank ASC
    """, (session_id,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [
        {
            "name": r[0],
            "cuisine_type": r[1],
            "price_range": r[2],
            "avg_rating": r[3],
            "match_score": r[4],
            "match_reason": r[5],
            "rank": r[6]
        } for r in rows
    ]

# --- ENDPOINT 3: Save user preferences ---
@app.post("/preferences")
def save_preferences(data: dict):
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO REMY_DB.CORE.USER_PREFERENCES 
        (preference_id, user_id, occasion, group_size, vibe, budget_range)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        data["preference_id"],
        data["user_id"],
        data["occasion"],
        data["group_size"],
        data["vibe"],
        data["budget_range"]
    ))
    conn.commit()
    cursor.close()
    conn.close()
    return {"status": "saved"}

# --- ENDPOINT 4: AI Restaurant Recommendation ---
@app.post("/recommend")
def get_ai_recommendation(preferences: dict):
    import json

    # Pull restaurants from Snowflake
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name, cuisine_type, price_range, avg_rating,
               noise_level, has_outdoor_seating, has_parking, accepts_reservations
        FROM REMY_DB.CORE.RESTAURANTS
    """)
    rows = cursor.fetchmany(20)
    cursor.close()
    conn.close()

    # Format restaurant list for the AI prompt
    restaurant_list = "\n".join([
        f"- {r[0]} | Cuisine: {r[1]} | Price: {r[2]} | Rating: {r[3]} | "
        f"Noise: {r[4]} | Outdoor: {r[5]} | Parking: {r[6]} | Reservations: {r[7]}"
        for r in rows
    ])

    # Build the AI prompt
    prompt = f"""
You are Remy, a friendly restaurant recommendation assistant.

A user has shared the following preferences:
- Occasion: {preferences.get('occasion')}
- Group size: {preferences.get('group_size')}
- Cuisine preferences: {preferences.get('cuisines')}
- Dietary restrictions: {preferences.get('dietary_restrictions')}
- Vibe: {preferences.get('vibe')}
- Budget per person: {preferences.get('budget_range')}
- Max travel time: {preferences.get('max_travel_time')}
- Noise preference: {preferences.get('noise_preference')}
- Must haves: {preferences.get('must_haves')}

Here are the available restaurants:
{restaurant_list}

Return ONLY a JSON array with no explanation, no markdown, no extra text.
Just the raw JSON array like this:
[
  {{
    "name": "Restaurant Name",
    "cuisine_type": "Cuisine",
    "price_range": "$$",
    "avg_rating": 4.7,
    "match_reason": "Perfect for date night"
  }},
  {{
    "name": "Restaurant Name 2",
    "cuisine_type": "Cuisine",
    "price_range": "$$$",
    "avg_rating": 4.5,
    "match_reason": "Trendy and Instagrammable"
  }},
  {{
    "name": "Restaurant Name 3",
    "cuisine_type": "Cuisine",
    "price_range": "$",
    "avg_rating": 4.3,
    "match_reason": "Great for groups"
  }}
]
"""

    # Call Groq
    client = Groq(api_key=os.getenv("GROQ_API_KEY"))
    response = client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7
    )

    # Parse the response
    raw = response.choices[0].message.content.strip()
    clean = raw.replace("```json", "").replace("```", "").strip()
    start = clean.find("[")
    end = clean.rfind("]") + 1
    json_str = clean[start:end]
    recommendations = json.loads(json_str)

    return {"recommendations": recommendations}