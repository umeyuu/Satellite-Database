from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

load_dotenv()
import os

HOST = os.getenv("PLANETSCALE_HOST")
USER = os.getenv("USERNAME")
PASSWD = os.getenv("PASSWORD")
DB = os.getenv("DATABASE")

# データベース接続
ENGINE = create_engine(
    f"mysql://{USER}:{PASSWD}@{HOST}/{DB}?ssl_mode=VERIFY_IDENTITY",
    connect_args={"ssl": {"ca": "/etc/ssl/cert.pem"}},
)

session = sessionmaker(autocommit=False,
                       autoflush=True,
                       expire_on_commit=False,
                       bind=ENGINE)
# modelで使用する
Base = declarative_base()
# Base.query = session.query_property()