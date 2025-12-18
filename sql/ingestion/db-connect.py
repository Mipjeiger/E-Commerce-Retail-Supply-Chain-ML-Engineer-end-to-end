import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# Set default values jika environment variable tidak ada
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@localhost:5432/{DB_NAME}"
)


# usage
if __name__ == "__main__":
    with engine.connect() as connection:
        result = connection.execute("SELECT current_user, current_database();")
        print(result.fetchone())
