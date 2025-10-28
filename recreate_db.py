from database import Base, engine

# Drop and recreate all tables from the models
print("Dropping existing database tables (if any)...")
Base.metadata.drop_all(bind=engine)
print("Creating database tables...")
Base.metadata.create_all(bind=engine)
print("Database tables created successfully!")