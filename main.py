from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, HttpUrl
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
import random
import string

# ===================== Database Setup =====================
DATABASE_URL = "sqlite:///./urls.db"  # Use PostgreSQL by changing URL

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ===================== Database Model =====================
class URL(Base):
    __tablename__ = "urls"

    id = Column(Integer, primary_key=True, index=True)
    original_url = Column(String, nullable=False)
    short_code = Column(String(8), unique=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    expiry_date = Column(DateTime)
    visit_count = Column(Integer, default=0)
    last_visited_at = Column(DateTime, nullable=True)

Base.metadata.create_all(bind=engine)

# ===================== FastAPI App =====================
app = FastAPI(title="URL Shortener with Analytics")

# ===================== Schemas =====================
class URLCreate(BaseModel):
    original_url: HttpUrl
    expiry_days: int = 30  # default expiry 30 days

class URLInfo(BaseModel):
    short_code: str
    original_url: str
    expiry_date: datetime

# ===================== Helper Functions =====================
def generate_code(length=6):
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for _ in range(length))

def get_unique_code(db, length=6):
    code = generate_code(length)
    while db.query(URL).filter(URL.short_code == code).first():
        code = generate_code(length)
    return code

# ===================== Routes =====================
@app.post("/shorten", response_model=URLInfo)
def shorten_url(payload: URLCreate):
    db = SessionLocal()
    try:
        code = get_unique_code(db)
        expiry = datetime.utcnow() + timedelta(days=payload.expiry_days)
        new_url = URL(
            original_url=str(payload.original_url),  # <--- convert here
            short_code=code,
            expiry_date=expiry
        )
        db.add(new_url)
        db.commit()
        db.refresh(new_url)
        return URLInfo(
            short_code=new_url.short_code,
            original_url=new_url.original_url,
            expiry_date=new_url.expiry_date
        )
    finally:
        db.close()


@app.get("/r/{code}")
def redirect_url(code: str):
    db = SessionLocal()
    try:
        url = db.query(URL).filter(URL.short_code == code).first()
        if not url:
            raise HTTPException(status_code=404, detail="Shortcode not found")
        if url.expiry_date and url.expiry_date < datetime.utcnow():
            raise HTTPException(status_code=410, detail="URL expired")
        # Update analytics
        url.visit_count += 1
        url.last_visited_at = datetime.utcnow()
        db.commit()
        return RedirectResponse(url.original_url)
    finally:
        db.close()
