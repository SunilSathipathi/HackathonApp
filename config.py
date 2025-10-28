from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Mendix API Configuration
    mendix_api_base_url: str = "http://localhost:8080/rest/employeeservice/v1"
    mendix_api_username: str
    mendix_api_password: str
    
    # OpenAI Configuration
    openai_api_key: str
    openai_model: str = "gpt-4o-mini"
    openai_embedding_model: str = "text-embedding-3-small"
    
    # Database Configuration
    database_url: str = "sqlite:///./employee_data.db"
    vector_db_path: str = "./vector_store"
    enable_vector_search: bool = True
    
    # Scheduler Configuration
    sync_interval_minutes: int = 5
    
    # API Configuration
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    
    # Logging
    log_level: str = "INFO"
    
    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()