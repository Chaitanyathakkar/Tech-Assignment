import asyncio
import aiosqlite
import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from datetime import datetime

DB_NAME = "posts.db"
EXTERNAL_API_URL = "https://jsonplaceholder.typicode.com/posts"
FETCH_INTERVAL = 15 * 60  # 15 minutes


# ==============================================================
#  FASTAPI APP
# ==============================================================

app = FastAPI(title="Post Fetcher API")  # <--- THIS MUST EXIST AT TOP LEVEL


# ==============================================================
#  INIT DATABASE
# ==============================================================

async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY,
                userId INTEGER,
                title TEXT,
                body TEXT,
                fetched_at TEXT
            )
        """)
        await db.commit()


# ==============================================================
#  FETCH EXTERNAL API AND STORE NEW POSTS
# ==============================================================

async def fetch_and_store_posts():
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(EXTERNAL_API_URL)
            posts = response.json()

        async with aiosqlite.connect(DB_NAME) as db:
            for post in posts:
                # Skip duplicates
                cursor = await db.execute("SELECT id FROM posts WHERE id = ?", (post["id"],))
                exists = await cursor.fetchone()

                if exists:
                    continue

                await db.execute("""
                    INSERT INTO posts (id, userId, title, body, fetched_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    post["id"],
                    post["userId"],
                    post["title"],
                    post["body"],
                    datetime.utcnow().isoformat()
                ))

            await db.commit()

        print(f"[INFO] Fetch completed at {datetime.utcnow()}")

    except Exception as e:
        print(f"[ERROR] {e}")


# ==============================================================
#  BACKGROUND SCHEDULER
# ==============================================================

async def scheduler():
    while True:
        await fetch_and_store_posts()
        await asyncio.sleep(FETCH_INTERVAL)


@app.on_event("startup")
async def startup_event():
    await init_db()
    asyncio.create_task(scheduler())  # Background task


# ==============================================================
#  API ENDPOINT: /posts
# ==============================================================

@app.get("/posts")
async def get_posts():
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("""
            SELECT id, userId, title, body, fetched_at
            FROM posts
            ORDER BY fetched_at DESC
            LIMIT 10
        """)
        rows = await cursor.fetchall()

    posts = [
        {
            "id": r[0],
            "userId": r[1],
            "title": r[2],
            "body": r[3],
            "fetched_at": r[4]
        }
        for r in rows
    ]

    return JSONResponse(posts)



# ================= To Run this program ==================
#  1.Open terminal
#  2.Write uvicorn main:app --reload(app will start)
#  3.You will see in the logs fetch completed and to see the fetched posts you can find in the "post.db"