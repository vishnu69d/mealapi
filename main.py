from fastapi import FastAPI, HTTPException
import httpx
import os
import asyncpg
import random
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
import logging
from contextlib import asynccontextmanager

# Configure logging first
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
logger.info("Environment variables loaded")

# Database and API configuration
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
logger.info(f"Database URL loaded: {'Yes' if SUPABASE_DB_URL else 'No'}")

# Ensure correct database URL
# if not SUPABASE_DB_URL:
#     # Hard-code the URL for testing if env var not available
#     SUPABASE_DB_URL = "postgresql://postgres.ruumkzaylpbowqazjxjb:31JuAanqV5FOPT8X@aws-0-ap-south-1.pooler.supabase.com:6543/postgres"
#     logger.info("Using hardcoded database URL")

MEALDB_API_URL = "https://www.themealdb.com/api/json/v1/1/search.php?s="

# Global connection pool
db_pool = None

# Lifespan context manager for startup and shutdown events
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize the database connection pool on startup
    global db_pool
    logger.info("Creating database connection pool")
    try:
        db_pool = await asyncpg.create_pool(
            SUPABASE_DB_URL,
            min_size=5,
            max_size=20,
            statement_cache_size=0  # Disable prepared statement caching
        )
        logger.info("Database connection pool created successfully")
    except Exception as e:
        logger.error(f"Failed to create database connection pool: {str(e)}")
        raise
    
    yield  # Application runs here
    
    # Close the connection pool on shutdown
    if db_pool:
        logger.info("Closing database connection pool")
        await db_pool.close()

# Create FastAPI app with lifespan
app = FastAPI(lifespan=lifespan)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allow all headers
)

# Async function to get meal data from MealDB API
async def fetch_mealdb_data(meal_name: str):
    logger.info(f"Fetching data from MealDB API for: {meal_name}")
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(f"{MEALDB_API_URL}{meal_name}")
            response.raise_for_status()
            data = response.json()
            if data["meals"]:
                return [{**meal, "source": "MealDB"} for meal in data["meals"]]
        except Exception as e:
            logger.error(f"MealDB API error: {str(e)}")
    return []

# API Endpoint to fetch meal details
@app.get("/meals/{meal_name}")
async def get_meal(meal_name: str):
    MAX_RESULTS = 20
    
    logger.info(f"Processing request for meal: {meal_name}")
    
    # Fetch data from MealDB API asynchronously
    mealdb_meals = await fetch_mealdb_data(meal_name)
    logger.info(f"Found {len(mealdb_meals)} meals from MealDB")
    
    # Fetch data from Supabase database
    supabase_meals = []
    try:
        # Use the connection pool instead of creating a new connection
        async with db_pool.acquire() as conn:
            logger.info("Database connection acquired from pool")
            
            db_meals = await conn.fetch("""
                SELECT *, 
                    CASE WHEN LOWER(name) = LOWER($1) THEN 1 ELSE 2 END as match_priority
                FROM extra_meals 
                WHERE LOWER(name) LIKE LOWER($2)
                ORDER BY match_priority ASC
            """, meal_name, f"%{meal_name}%")
            
            logger.info(f"Found {len(db_meals)} meals from database")
            
            if db_meals:
                for meal in db_meals:
                    formatted_meal = {
                        "idMeal": meal["id"],
                        "strMeal": meal["name"],
                        "strCategory": meal["category"],
                        "strArea": meal["area"],
                        "strInstructions": meal["instructions"].split("', '"),
                        "strMealThumb": meal["images"],
                        "strIngredients": meal["ingredients"].strip("[]").replace("'", "").split(", ") if meal["ingredients"] else [],
                        "source": "Supabase DB"
                    }
                    supabase_meals.append(formatted_meal)

    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    if not mealdb_meals and not supabase_meals:
        logger.warning(f"No meals found for: {meal_name}")
        raise HTTPException(status_code=404, detail="Meal not found in MealDB or Supabase database.")

    # Balanced random selection
    final_meals = []
    total_meals_needed = MAX_RESULTS

    if mealdb_meals and supabase_meals:
        # Calculate balanced proportions
        mealdb_proportion = min(len(mealdb_meals), total_meals_needed // 2)
        supabase_proportion = total_meals_needed - mealdb_proportion

        # Random selection from MealDB
        if len(mealdb_meals) > mealdb_proportion:
            final_meals.extend(random.sample(mealdb_meals, mealdb_proportion))
        else:
            final_meals.extend(mealdb_meals)

        # Random selection from Supabase
        if len(supabase_meals) > supabase_proportion:
            final_meals.extend(random.sample(supabase_meals, supabase_proportion))
        else:
            final_meals.extend(supabase_meals)

    else:
        # If we only have results from one source, use those
        source_meals = mealdb_meals if mealdb_meals else supabase_meals
        if len(source_meals) > 0:
            final_meals = random.sample(source_meals, min(len(source_meals), total_meals_needed))
        else:
            final_meals = []

    # Shuffle the final list to mix results from both sources
    random.shuffle(final_meals)

    # Return combined results
    return {
        "total_available": len(mealdb_meals) + len(supabase_meals),
        "mealdb_count": len(mealdb_meals),
        "supabase_count": len(supabase_meals),
        "returned_results": len(final_meals),
        "max_results": MAX_RESULTS,
        "data": final_meals
    }

# API Endpoint to add a meal to the database
@app.post("/add_meal/")
async def add_meal(name: str, category: str, area: str, instructions: str, ingredients: str, image: str):
    try:
        # Use the connection pool instead of creating a new connection
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO extra_meals (name, category, area, instructions, ingredients, image) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (name) DO NOTHING",
                name, category, area, instructions, ingredients, image
            )
        return {"message": "Meal added successfully"}
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)