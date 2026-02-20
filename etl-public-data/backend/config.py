from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "etl_public"
    postgres_user: str = "etl_user"
    postgres_password: str = "etl_password"

    air_quality_api_key: str = ""
    weather_api_key: str = ""
    subway_api_key: str = ""

    use_mock_data: bool = True

    etl_cron_hour: str = "*"
    etl_cron_minute: str = "0"
    quality_report_hour: str = "1"
    quality_report_minute: str = "0"

    backend_host: str = "0.0.0.0"
    backend_port: int = 8000

    @property
    def database_url(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    class Config:
        env_file = ".env"


settings = Settings()
