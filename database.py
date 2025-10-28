from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool
from config import settings
from models import Base
import logging

logger = logging.getLogger(__name__)

# Create database engine
if settings.database_url.startswith("sqlite"):
    # SQLite-specific configuration
    engine = create_engine(
        settings.database_url,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        echo=False
    )
else:
    # PostgreSQL or other databases
    engine = create_engine(
        settings.database_url,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
        echo=False
    )

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    """Initialize database - create all tables."""
    try:
        Base.metadata.create_all(bind=engine)
        # Ensure new columns exist when running against an existing DB
        try:
            inspector = inspect(engine)
            employee_columns = [c['name'] for c in inspector.get_columns('employees')]
            if 'department_id' not in employee_columns:
                with engine.connect() as conn:
                    try:
                        conn.execute(text("ALTER TABLE employees ADD COLUMN department_id VARCHAR(50)"))
                        logger.info("Added employees.department_id column via migration")
                    except Exception as e:
                        # Column may already exist or DB may not support ALTER (should be rare)
                        logger.warning(f"Could not add employees.department_id column: {e}")
        except Exception as e:
            logger.warning(f"Schema inspection failed during init: {e}")
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Error creating database tables: {e}")
        raise


def get_db() -> Session:
    """Dependency to get database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_db_session() -> Session:
    """Get a database session for use outside of FastAPI dependency injection."""
    return SessionLocal()