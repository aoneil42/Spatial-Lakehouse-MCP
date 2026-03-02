"""Configuration via environment variables with SLM_ prefix."""

from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    model_config = {"env_prefix": "SLM_"}

    # LakeKeeper connection
    catalog_uri: str = Field(
        default="http://localhost:8181",
        description="LakeKeeper REST catalog endpoint",
    )
    catalog_warehouse: str = Field(
        default="warehouse",
        description="Warehouse name registered in LakeKeeper",
    )
    catalog_alias: str = Field(
        default="lakehouse",
        description="DuckDB alias for the attached Iceberg catalog",
    )

    # LakeKeeper auth (OAuth2) — empty strings mean no auth
    catalog_client_id: str = Field(default="", description="OAuth2 client ID")
    catalog_client_secret: str = Field(default="", description="OAuth2 client secret")
    catalog_oauth2_scope: str = Field(default="", description="OAuth2 scope")
    catalog_oauth2_server_uri: str = Field(
        default="",
        description="OAuth2 token endpoint (leave empty if LakeKeeper has no auth)",
    )
    # If you already have a bearer token instead of OAuth2 credentials:
    catalog_token: str = Field(default="", description="Bearer token (alternative to OAuth2)")

    # Iceberg access delegation
    access_delegation_mode: str = Field(
        default="none",
        description="'none' for host-side (bypasses LakeKeeper signing), "
                    "'vended-credentials' for Docker (uses LakeKeeper's S3 credentials)"
    )

    # Garage S3 connection
    s3_endpoint: str = Field(
        default="localhost:3900",
        description="Garage S3 endpoint (host:port, no scheme)",
    )
    s3_access_key_id: str = Field(default="", alias="GARAGE_KEY_ID")
    s3_secret_access_key: str = Field(default="", alias="GARAGE_SECRET_KEY")
    s3_region: str = Field(default="garage", description="S3 region")
    s3_use_ssl: bool = Field(default=False, description="Use HTTPS for S3")
    s3_url_style: str = Field(default="path", description="S3 URL style: path or vhost")

    # Query limits
    max_result_rows: int = Field(default=5000, ge=1, le=50000,
        description="Hard ceiling on rows returned by any tool")
    default_result_rows: int = Field(default=100, ge=1, le=5000,
        description="Default LIMIT when tool caller doesn't specify")
    max_query_length: int = Field(default=8000, ge=100, le=50000)
    query_timeout_seconds: int = Field(default=30, ge=1, le=300)

    # Server
    server_name: str = Field(default="spatial-lakehouse-mcp")
    server_host: str = Field(default="0.0.0.0")
    server_port: int = Field(default=8082)
    transport: str = Field(default="streamable-http",
        description="MCP transport: 'streamable-http' for Docker/remote, 'stdio' for Claude Desktop")


settings = Settings()
