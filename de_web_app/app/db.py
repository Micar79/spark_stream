"""Database helpers (SQLAlchemy) — simple engine factory."""
import os
from sqlalchemy import create_engine

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://de_user:de_pass@localhost:5432/de_db")

engine = create_engine(DATABASE_URL, echo=False)
