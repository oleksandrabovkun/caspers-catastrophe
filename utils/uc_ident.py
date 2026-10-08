"""Quote Unity Catalog names for Spark SQL.

Catalogs like ``full-databricks-demo`` are valid UC names but illegal unquoted
SQL identifiers. Always use ``uc(...)`` in Spark SQL / DBSQL text.
REST/SDK ``full_name`` values stay unquoted (``cat.schema.table``).
"""


def uc(*parts: str) -> str:
    bits: list[str] = []
    for part in parts:
        for bit in str(part).replace("`", "").split("."):
            bit = bit.strip()
            if bit:
                bits.append(f"`{bit}`")
    return ".".join(bits)
