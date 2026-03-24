from app.utils import connect_to_snowflake


def _connect_snowflake(database: str | None = None, schema: str | None = None):
    kwargs = {}
    if database is not None:
        kwargs["database"] = database
    if schema is not None:
        kwargs["schema"] = schema
    return connect_to_snowflake(**kwargs)


def connect_snowflake(database: str | None = None, schema: str | None = None):
    kwargs = {}
    if database is not None:
        kwargs["database"] = database
    if schema is not None:
        kwargs["schema"] = schema
    return connect_to_snowflake(**kwargs)
