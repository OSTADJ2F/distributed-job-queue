from app.config import Settings


def test_managed_postgres_url_selects_asyncpg_driver() -> None:
    settings = Settings(
        database_url="postgresql://queue:secret@database.example/queue",
        _env_file=None,
    )
    assert settings.database_url == ("postgresql+asyncpg://queue:secret@database.example/queue")


def test_explicit_sqlalchemy_driver_is_preserved() -> None:
    url = "postgresql+asyncpg://queue:secret@localhost/queue"
    assert Settings(database_url=url, _env_file=None).database_url == url
