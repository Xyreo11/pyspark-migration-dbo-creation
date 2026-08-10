# =============================================================================
# PySpark DDL Utility : create_gold_dbo_tables
#
#   Parse Oracle CREATE TABLE / constraint DDL and create equivalent Delta
#   tables in the Fabric Gold lakehouse dbo schema.
#
# Usage patterns:
#   1. Default project run:
#        python create_gold_dbo_tables.py --dry-run
#      Then run in Fabric without --dry-run to create the Delta tables.
#
#   2. New arbitrary Oracle DDL file:
#        python create_gold_dbo_tables.py --ddl-file path/to/new_table.sql --dry-run
#
#   3. Fabric notebook:
#        Set DDL_INPUT_PATH or ORACLE_DDL_TEXT below, then run all cells.
#        For sectioned execution, run:
#          tables = run_ddl_section("parse")
#          tables = run_ddl_section("create")
#          tables = run_ddl_section("validate")
#
# Notes:
#   - Oracle indexes, row movement, and partition clauses are physical storage
#     directives. Delta table creation keeps the logical schema and stores
#     Oracle constraints as metadata, with optional validation checks.
#   - Table names are created in lower_snake_case to match the existing Fabric
#     lakehouse shape, while source column names remain Oracle-compatible.
# =============================================================================

from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


# =============================================================================
# SECTION 1 - NOTEBOOK PARAMETERS / FABRIC PATHS
# =============================================================================

ONELAKE_ROOT = "abfss://FCI_Digitus@onelake.dfs.fabric.microsoft.com"
LAKEHOUSE = "Gold"
SCHEMA_NAME = "dbo"
TARGET_TABLE_ROOT = f"{ONELAKE_ROOT}/{LAKEHOUSE}.Lakehouse/Tables/{SCHEMA_NAME}"

# Set this to a Fabric Files path, local workspace path, or leave the default for
# the supplied migration backlog.
DDL_INPUT_PATH = "source/ddl/datap_tables.txt"

# Optional direct DDL input. If populated, this takes precedence over
# DDL_INPUT_PATH. Useful when a single new Oracle CREATE TABLE statement is
# pasted into a Fabric notebook.
ORACLE_DDL_TEXT = ""

# Creation behavior.
REPLACE_EXISTING_TABLES = False
FAIL_ON_SCHEMA_MISMATCH = True
WRITE_CONSTRAINT_METADATA = True
RUN_SCHEMA_VALIDATION = True
RUN_CONSTRAINT_VALIDATION = True
WRITE_SCHEMA_MISMATCH_LOG = True
CONTINUE_ON_TABLE_ERROR = True
SCHEMA_MISMATCH_LOG_PATH = (
    f"{ONELAKE_ROOT}/{LAKEHOUSE}.Lakehouse/Files/ddl_validation/schema_mismatches"
)
SCHEMA_MISMATCHES: list[dict[str, Any]] = []
DDL_RUN_ISSUES: list[dict[str, Any]] = []

# Notebook section control. Use these when running this file as notebook cells:
#   parse    - read/parse/summarize only
#   create   - create/register tables only
#   validate - validate already-created tables only
#   all      - create/register and validate
RUN_SECTION = "all"

# The current Fabric scripts use identifiers such as Gold.dbo.customer_master.
# Set to False if the notebook is already attached to Gold and only dbo.table is
# valid in your session.
USE_THREE_PART_IDENTIFIER = True

# Optional per-table naming overrides. Keys are source Oracle table names.
TABLE_NAME_OVERRIDES: dict[str, str] = {}

REMOTE_PATH_PREFIXES = (
    "abfss://",
    "abfs://",
    "wasbs://",
    "wasb://",
    "s3://",
    "s3a://",
)


# =============================================================================
# SECTION 2 - MODEL
# =============================================================================

@dataclass
class ColumnDef:
    name: str
    oracle_type: str
    nullable: bool = True
    default: str | None = None


@dataclass
class PrimaryKeyDef:
    name: str | None
    columns: list[str]


@dataclass
class UniqueKeyDef:
    name: str | None
    columns: list[str]


@dataclass
class ForeignKeyDef:
    name: str | None
    columns: list[str]
    ref_schema: str | None
    ref_table: str
    ref_columns: list[str]


@dataclass
class CheckConstraintDef:
    name: str | None
    expression: str


@dataclass
class UniqueIndexDef:
    name: str
    columns: list[str]


@dataclass
class TableDef:
    source_schema: str | None
    source_name: str
    target_name: str
    columns: list[ColumnDef] = field(default_factory=list)
    primary_key: PrimaryKeyDef | None = None
    unique_keys: list[UniqueKeyDef] = field(default_factory=list)
    foreign_keys: list[ForeignKeyDef] = field(default_factory=list)
    checks: list[CheckConstraintDef] = field(default_factory=list)
    unique_indexes: list[UniqueIndexDef] = field(default_factory=list)

    @property
    def not_null_columns(self) -> list[str]:
        return [column.name for column in self.columns if not column.nullable]

    @property
    def defaults(self) -> dict[str, str]:
        return {
            column.name: column.default
            for column in self.columns
            if column.default is not None
        }


# =============================================================================
# SECTION 3 - PARSING HELPERS
# =============================================================================

OBJECT_NAME_PATTERN = (
    r'(?:"(?P<schema_q>[^"]+)"\s*\.\s*"(?P<table_q>[^"]+)"|'
    r'(?P<schema_u>[A-Za-z_][\w$#]*)\s*\.\s*(?P<table_u>[A-Za-z_][\w$#]*)|'
    r'"(?P<table_only_q>[^"]+)"|'
    r'(?P<table_only_u>[A-Za-z_][\w$#]*))'
)


def strip_sql_comments(raw_text: str) -> str:
    result: list[str] = []
    in_single_quote = False
    in_double_quote = False
    index = 0

    while index < len(raw_text):
        char = raw_text[index]
        next_char = raw_text[index + 1] if index + 1 < len(raw_text) else ""

        if in_single_quote:
            result.append(char)
            if char == "'" and next_char == "'":
                result.append(next_char)
                index += 2
                continue
            if char == "'":
                in_single_quote = False
            index += 1
            continue

        if in_double_quote:
            result.append(char)
            if char == '"':
                in_double_quote = False
            index += 1
            continue

        if char == "'":
            in_single_quote = True
            result.append(char)
        elif char == '"':
            in_double_quote = True
            result.append(char)
        elif char == "-" and next_char == "-":
            while index < len(raw_text) and raw_text[index] not in "\r\n":
                index += 1
            result.append("\n")
            continue
        elif char == "/" and next_char == "*":
            index += 2
            while index + 1 < len(raw_text) and not (raw_text[index] == "*" and raw_text[index + 1] == "/"):
                if raw_text[index] in "\r\n":
                    result.append("\n")
                index += 1
            index += 2
            continue
        else:
            result.append(char)

        index += 1

    return "".join(result)


def normalize_ddl_text(raw_text: str) -> str:
    text = strip_sql_comments(raw_text).replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"/\s*\n\s+", "/", text)
    text = re.sub(r"'\s*\n\s*'", "", text)
    return text


def to_snake_identifier(name: str) -> str:
    candidate = re.sub(r"[^0-9A-Za-z]+", "_", name.strip())
    candidate = re.sub(r"_+", "_", candidate).strip("_").lower()
    if not candidate:
        raise ValueError(f"Cannot derive a Fabric table name from {name!r}")
    if candidate[0].isdigit():
        candidate = f"t_{candidate}"
    return candidate


def clean_identifier(identifier: str | None) -> str | None:
    if identifier is None:
        return None
    value = identifier.strip().strip('"')
    return value.upper()


def parse_object_name(raw_name: str) -> tuple[str | None, str]:
    match = re.match(rf"\s*{OBJECT_NAME_PATTERN}\s*$", raw_name, re.IGNORECASE)
    if not match:
        raise ValueError(f"Unable to parse Oracle object name: {raw_name!r}")

    groups = match.groupdict()
    schema = clean_identifier(groups.get("schema_q") or groups.get("schema_u"))
    table = clean_identifier(
        groups.get("table_q")
        or groups.get("table_u")
        or groups.get("table_only_q")
        or groups.get("table_only_u")
    )
    if table is None:
        raise ValueError(f"Unable to parse Oracle table name: {raw_name!r}")
    return schema, table


def quoted_columns(raw_columns: str) -> list[str]:
    quoted = re.findall(r'"([^"]+)"', raw_columns)
    if quoted:
        return [column.upper() for column in quoted]

    return [
        column.strip().strip('"').upper()
        for column in raw_columns.split(",")
        if column.strip()
    ]


def find_matching_paren(text: str, open_index: int) -> int:
    depth = 0
    in_single_quote = False
    in_double_quote = False
    index = open_index

    while index < len(text):
        char = text[index]
        next_char = text[index + 1] if index + 1 < len(text) else ""

        if in_single_quote:
            if char == "'" and next_char == "'":
                index += 2
                continue
            if char == "'":
                in_single_quote = False
            index += 1
            continue

        if in_double_quote:
            if char == '"':
                in_double_quote = False
            index += 1
            continue

        if char == "'":
            in_single_quote = True
        elif char == '"':
            in_double_quote = True
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index

        index += 1

    raise ValueError("Unable to find the closing parenthesis for CREATE TABLE.")


def split_top_level_commas(text: str) -> list[str]:
    parts: list[str] = []
    start = 0
    depth = 0
    in_single_quote = False
    in_double_quote = False
    index = 0

    while index < len(text):
        char = text[index]
        next_char = text[index + 1] if index + 1 < len(text) else ""

        if in_single_quote:
            if char == "'" and next_char == "'":
                index += 2
                continue
            if char == "'":
                in_single_quote = False
            index += 1
            continue

        if in_double_quote:
            if char == '"':
                in_double_quote = False
            index += 1
            continue

        if char == "'":
            in_single_quote = True
        elif char == '"':
            in_double_quote = True
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            parts.append(text[start:index].strip())
            start = index + 1

        index += 1

    final_part = text[start:].strip()
    if final_part:
        parts.append(final_part)
    return parts


def extract_parenthesized_after(keyword: str, text: str) -> str | None:
    match = re.search(keyword, text, re.IGNORECASE)
    if not match:
        return None

    open_index = text.find("(", match.end())
    if open_index < 0:
        return None

    close_index = find_matching_paren(text, open_index)
    return text[open_index + 1:close_index]


def extract_constraint_name(item: str) -> str | None:
    match = re.search(r'\bCONSTRAINT\s+(?:"([^"]+)"|([A-Za-z_][\w$#]*))', item, re.IGNORECASE)
    if not match:
        return None
    return clean_identifier(match.group(1) or match.group(2))


def compact_sql(sql_text: str) -> str:
    return re.sub(r"\s+", " ", sql_text.strip())


def parse_primary_key(item: str) -> PrimaryKeyDef | None:
    if not re.search(r"\bPRIMARY\s+KEY\b", item, re.IGNORECASE):
        return None

    raw_columns = extract_parenthesized_after(r"\bPRIMARY\s+KEY\b", item)
    if raw_columns is None:
        return None
    return PrimaryKeyDef(name=extract_constraint_name(item), columns=quoted_columns(raw_columns))


def parse_unique_key(item: str) -> UniqueKeyDef | None:
    if not re.search(r"\bUNIQUE\b", item, re.IGNORECASE):
        return None
    if re.search(r"\bCREATE\s+UNIQUE\s+INDEX\b", item, re.IGNORECASE):
        return None

    raw_columns = extract_parenthesized_after(r"\bUNIQUE\b", item)
    if raw_columns is None:
        return None
    return UniqueKeyDef(name=extract_constraint_name(item), columns=quoted_columns(raw_columns))


def parse_foreign_key(item: str) -> ForeignKeyDef | None:
    if not re.search(r"\bFOREIGN\s+KEY\b", item, re.IGNORECASE):
        return None

    raw_columns = extract_parenthesized_after(r"\bFOREIGN\s+KEY\b", item)
    ref_match = re.search(
        rf"\bREFERENCES\s+(?P<name>{OBJECT_NAME_PATTERN})\s*\(",
        item,
        re.IGNORECASE,
    )
    if raw_columns is None or not ref_match:
        return None

    ref_schema, ref_table = parse_object_name(ref_match.group("name"))
    ref_open_index = item.find("(", ref_match.end() - 1)
    ref_close_index = find_matching_paren(item, ref_open_index)
    ref_columns = item[ref_open_index + 1:ref_close_index]

    return ForeignKeyDef(
        name=extract_constraint_name(item),
        columns=quoted_columns(raw_columns),
        ref_schema=ref_schema,
        ref_table=ref_table,
        ref_columns=quoted_columns(ref_columns),
    )


def parse_check_constraint(item: str) -> CheckConstraintDef | None:
    if not re.search(r"\bCHECK\b", item, re.IGNORECASE):
        return None

    expression = extract_parenthesized_after(r"\bCHECK\b", item)
    if expression is None:
        return None
    return CheckConstraintDef(
        name=extract_constraint_name(item),
        expression=compact_sql(expression),
    )


def parse_unique_index(item: str) -> tuple[str, UniqueIndexDef] | None:
    match = re.search(
        rf'\bCREATE\s+UNIQUE\s+INDEX\s+'
        rf'(?:(?:"[^"]+"|[A-Za-z_][\w$#]*)\s*\.\s*)?'
        rf'(?:"(?P<index_q>[^"]+)"|(?P<index_u>[A-Za-z_][\w$#]*))'
        rf"\s+ON\s+(?P<table>{OBJECT_NAME_PATTERN})\s*\(",
        item,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None

    _, table_name = parse_object_name(match.group("table"))
    open_index = item.find("(", match.end() - 1)
    close_index = find_matching_paren(item, open_index)
    columns = quoted_columns(item[open_index + 1:close_index])
    index_name = clean_identifier(match.group("index_q") or match.group("index_u")) or ""
    return table_name, UniqueIndexDef(name=index_name, columns=columns)


def extract_oracle_type(rest: str) -> str:
    type_patterns = [
        r"TIMESTAMP\s*(?:\(\s*\d+\s*\))?(?:\s+WITH(?:\s+LOCAL)?\s+TIME\s+ZONE)?",
        r"VARCHAR2\s*\(\s*\d+\s*(?:BYTE|CHAR)?\s*\)",
        r"NVARCHAR2\s*\(\s*\d+\s*\)",
        r"VARCHAR\s*\(\s*\d+\s*(?:BYTE|CHAR)?\s*\)",
        r"NCHAR\s*\(\s*\d+\s*\)",
        r"CHAR\s*\(\s*\d+\s*(?:BYTE|CHAR)?\s*\)",
        r"NUMBER\s*(?:\(\s*(?:\d+|\*)\s*(?:,\s*-?\d+\s*)?\))?",
        r"NUMERIC\s*(?:\(\s*(?:\d+|\*)\s*(?:,\s*-?\d+\s*)?\))?",
        r"DECIMAL\s*(?:\(\s*(?:\d+|\*)\s*(?:,\s*-?\d+\s*)?\))?",
        r"DOUBLE\s+PRECISION",
        r"BINARY_DOUBLE",
        r"BINARY_FLOAT",
        r"INTEGER",
        r"SMALLINT",
        r"BIGINT",
        r"FLOAT",
        r"DATE",
        r"CLOB",
        r"NCLOB",
        r"BLOB",
        r"RAW\s*\(\s*\d+\s*\)",
        r"LONG",
    ]

    for pattern in type_patterns:
        match = re.match(pattern, rest.strip(), re.IGNORECASE)
        if match:
            return compact_sql(match.group(0)).upper()

    fallback = re.match(r"[A-Za-z_][\w$#]*(?:\s*\([^)]*\))?", rest.strip())
    if fallback:
        return compact_sql(fallback.group(0)).upper()

    raise ValueError(f"Unable to parse Oracle column type from: {rest!r}")


def extract_default(rest_after_type: str) -> str | None:
    match = re.search(r"\bDEFAULT\b", rest_after_type, re.IGNORECASE)
    if not match:
        return None

    default_text = rest_after_type[match.end():].strip()
    stop_match = re.search(
        r"\s+\b(?:NOT\s+NULL|NULL|ENABLE|DISABLE|CONSTRAINT|PRIMARY\s+KEY|REFERENCES|CHECK)\b",
        default_text,
        re.IGNORECASE,
    )
    if stop_match:
        default_text = default_text[:stop_match.start()].strip()

    return compact_sql(default_text) if default_text else None


def parse_column(item: str) -> ColumnDef | None:
    if is_table_constraint(item):
        return None

    match = re.match(
        r'\s*(?:"(?P<quoted>[^"]+)"|(?P<plain>[A-Za-z_][\w$#]*))\s+(?P<rest>.+?)\s*$',
        item,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None

    name = clean_identifier(match.group("quoted") or match.group("plain"))
    rest = compact_sql(match.group("rest"))
    oracle_type = extract_oracle_type(rest)
    rest_after_type = rest[len(oracle_type):].strip()

    return ColumnDef(
        name=name or "",
        oracle_type=oracle_type,
        nullable=not bool(re.search(r"\bNOT\s+NULL\b", rest, re.IGNORECASE)),
        default=extract_default(rest_after_type),
    )


def is_table_constraint(item: str) -> bool:
    return bool(
        re.match(
            r"\s*(?:CONSTRAINT|PRIMARY\s+KEY|FOREIGN\s+KEY|CHECK|UNIQUE)\b",
            item,
            re.IGNORECASE,
        )
    )


def parse_inline_foreign_key(item: str, column_name: str) -> ForeignKeyDef | None:
    ref_match = re.search(
        rf"\bREFERENCES\s+(?P<name>{OBJECT_NAME_PATTERN})(?:\s*\()?",
        item,
        re.IGNORECASE,
    )
    if not ref_match:
        return None

    ref_schema, ref_table = parse_object_name(ref_match.group("name"))
    ref_columns: list[str] = []

    open_index = item.find("(", ref_match.end() - 1)
    if open_index >= 0:
        close_index = find_matching_paren(item, open_index)
        ref_columns = quoted_columns(item[open_index + 1:close_index])

    return ForeignKeyDef(
        name=extract_constraint_name(item),
        columns=[column_name],
        ref_schema=ref_schema,
        ref_table=ref_table,
        ref_columns=ref_columns,
    )


def apply_inline_constraints(table: TableDef, column: ColumnDef, item: str) -> None:
    if re.search(r"\bPRIMARY\s+KEY\b", item, re.IGNORECASE) and table.primary_key is None:
        table.primary_key = PrimaryKeyDef(
            name=extract_constraint_name(item),
            columns=[column.name],
        )

    if re.search(r"\bUNIQUE\b", item, re.IGNORECASE):
        table.unique_keys.append(
            UniqueKeyDef(
                name=extract_constraint_name(item),
                columns=[column.name],
            )
        )

    foreign_key = parse_inline_foreign_key(item, column.name)
    if foreign_key:
        table.foreign_keys.append(foreign_key)

    check_constraint = parse_check_constraint(item)
    if check_constraint:
        table.checks.append(check_constraint)


def find_create_table_blocks(text: str) -> list[tuple[str | None, str, str]]:
    blocks: list[tuple[str | None, str, str]] = []
    create_pattern = re.compile(
        rf"\bCREATE\s+(?:GLOBAL\s+TEMPORARY\s+)?TABLE\s+(?P<name>{OBJECT_NAME_PATTERN})",
        re.IGNORECASE,
    )

    for match in create_pattern.finditer(text):
        table_schema, table_name = parse_object_name(match.group("name"))
        open_index = text.find("(", match.end())
        if open_index < 0:
            raise ValueError(f"CREATE TABLE for {table_name} has no column list.")
        close_index = find_matching_paren(text, open_index)
        blocks.append((table_schema, table_name, text[open_index + 1:close_index]))

    return blocks


def parse_create_table_block(schema: str | None, name: str, body: str) -> TableDef:
    target_name = TABLE_NAME_OVERRIDES.get(name, to_snake_identifier(name))
    table = TableDef(source_schema=schema, source_name=name, target_name=target_name)

    for item in split_top_level_commas(body):
        if is_table_constraint(item):
            primary_key = parse_primary_key(item)
            if primary_key:
                table.primary_key = primary_key
                continue

            foreign_key = parse_foreign_key(item)
            if foreign_key:
                table.foreign_keys.append(foreign_key)
                continue

            check_constraint = parse_check_constraint(item)
            if check_constraint:
                table.checks.append(check_constraint)
                continue

            unique_key = parse_unique_key(item)
            if unique_key:
                table.unique_keys.append(unique_key)
                continue

        column = parse_column(item)
        if column:
            table.columns.append(column)
            apply_inline_constraints(table, column, item)

    if not table.columns:
        raise ValueError(f"No columns were parsed for Oracle table {name}.")

    return table


def parse_alter_table_constraints(text: str, tables_by_source_name: dict[str, TableDef]) -> None:
    alter_pattern = re.compile(
        rf"\bALTER\s+TABLE\s+(?P<table>{OBJECT_NAME_PATTERN})\s+ADD\s+CONSTRAINT\s+"
        rf"(?P<constraint>(?:\"[^\"]+\"|[A-Za-z_][\w$#]*))\s+(?P<body>.*?)(?="
        rf"\n\s*(?:ALTER\s+TABLE|CREATE\s+UNIQUE\s+INDEX|={10,}|TABLE:|CREATE\s+TABLE)\b|$)",
        re.IGNORECASE | re.DOTALL,
    )

    for match in alter_pattern.finditer(text):
        _, table_name = parse_object_name(match.group("table"))
        table = tables_by_source_name.get(table_name)
        if table is None:
            continue

        constraint_name = match.group("constraint").strip().strip('"').upper()
        item = f'CONSTRAINT "{constraint_name}" {match.group("body")}'

        primary_key = parse_primary_key(item)
        if primary_key:
            table.primary_key = primary_key
            continue

        foreign_key = parse_foreign_key(item)
        if foreign_key:
            table.foreign_keys.append(foreign_key)
            continue

        check_constraint = parse_check_constraint(item)
        if check_constraint:
            table.checks.append(check_constraint)
            continue

        unique_key = parse_unique_key(item)
        if unique_key:
            table.unique_keys.append(unique_key)


def parse_unique_indexes(text: str, tables_by_source_name: dict[str, TableDef]) -> None:
    index_pattern = re.compile(
        rf"\bCREATE\s+UNIQUE\s+INDEX\s+.*?(?=\n\s*(?:ALTER\s+TABLE|CREATE\s+UNIQUE\s+INDEX|"
        rf"={10,}|TABLE:|CREATE\s+TABLE)\b|$)",
        re.IGNORECASE | re.DOTALL,
    )

    for match in index_pattern.finditer(text):
        parsed = parse_unique_index(match.group(0))
        if not parsed:
            continue
        table_name, index_def = parsed
        table = tables_by_source_name.get(table_name)
        if table:
            table.unique_indexes.append(index_def)


def parse_oracle_ddl(ddl_text: str) -> list[TableDef]:
    text = normalize_ddl_text(ddl_text)
    tables = [
        parse_create_table_block(schema, table_name, body)
        for schema, table_name, body in find_create_table_blocks(text)
    ]

    tables_by_source_name = {table.source_name: table for table in tables}
    parse_alter_table_constraints(text, tables_by_source_name)
    parse_unique_indexes(text, tables_by_source_name)
    return tables


# =============================================================================
# SECTION 4 - SPARK / DELTA HELPERS
# =============================================================================

def quote_spark_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def table_identifier(table_name: str) -> str:
    if USE_THREE_PART_IDENTIFIER and LAKEHOUSE:
        parts = [LAKEHOUSE, SCHEMA_NAME, table_name]
    else:
        parts = [SCHEMA_NAME, table_name]
    return ".".join(quote_spark_identifier(part) for part in parts if part)


def table_path(table_name: str) -> str:
    return f"{TARGET_TABLE_ROOT.rstrip('/')}/{table_name}"


def is_remote_path(path: str) -> bool:
    return path.lower().startswith(REMOTE_PATH_PREFIXES)


def load_ddl_text(input_path: str | None = None, spark: Any | None = None) -> str:
    if ORACLE_DDL_TEXT.strip():
        return ORACLE_DDL_TEXT

    resolved_path = input_path or DDL_INPUT_PATH
    if not resolved_path:
        raise ValueError("Set ORACLE_DDL_TEXT or DDL_INPUT_PATH before running.")

    if is_remote_path(resolved_path):
        if spark is None:
            spark = build_spark_session()
        return "\n".join(row.value for row in spark.read.text(resolved_path).collect())

    path = Path(resolved_path)
    if not path.exists():
        if spark is not None:
            return "\n".join(row.value for row in spark.read.text(resolved_path).collect())
        raise FileNotFoundError(
            f"DDL input file was not found locally: {resolved_path}. "
            "For OneLake/abfss paths, run without --dry-run so Spark can read it."
        )

    return path.read_text(encoding="utf-8", errors="ignore")


def build_spark_session():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("create_gold_dbo_tables")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def oracle_type_to_spark_type(oracle_type: str):
    from pyspark.sql.types import (
        BinaryType,
        BooleanType,
        DateType,
        DecimalType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        TimestampType,
    )

    normalized = compact_sql(oracle_type).upper()

    if normalized.startswith(("VARCHAR", "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "CLOB", "NCLOB", "LONG")):
        return StringType()

    if normalized.startswith(("BLOB", "RAW")):
        return BinaryType()

    if normalized.startswith(("TIMESTAMP",)):
        return TimestampType()

    if normalized == "DATE":
        return DateType()

    if normalized in {"BINARY_DOUBLE", "BINARY_FLOAT", "DOUBLE PRECISION", "FLOAT"}:
        return DoubleType()

    if normalized in {"INTEGER", "INT", "SMALLINT"}:
        return IntegerType()

    if normalized == "BIGINT":
        return LongType()

    if normalized == "BOOLEAN":
        return BooleanType()

    numeric_match = re.match(
        r"(?:NUMBER|NUMERIC|DECIMAL)\s*(?:\(\s*(?P<precision>\d+|\*)\s*(?:,\s*(?P<scale>-?\d+)\s*)?\))?$",
        normalized,
    )
    if numeric_match:
        precision_text = numeric_match.group("precision")
        scale_text = numeric_match.group("scale")

        if precision_text is None:
            return DecimalType(38, 10)

        precision = 38 if precision_text == "*" else max(1, min(int(precision_text), 38))
        scale = int(scale_text) if scale_text is not None else 0
        scale = max(0, min(scale, precision))
        return DecimalType(precision, scale)

    print(f"    Warning: Unhandled Oracle type {oracle_type!r}; using StringType.")
    return StringType()


def build_spark_schema(table: TableDef):
    from pyspark.sql.types import StructField, StructType

    return StructType([
        StructField(
            column.name,
            oracle_type_to_spark_type(column.oracle_type),
            nullable=column.nullable,
        )
        for column in table.columns
    ])


def table_exists_at_path(spark: Any, path: str) -> bool:
    from delta.tables import DeltaTable

    return DeltaTable.isDeltaTable(spark, path)


def try_sql(spark: Any, statement: str, warning: str) -> bool:
    try:
        spark.sql(statement)
        return True
    except Exception as exc:  # pragma: no cover - depends on Fabric runtime
        print(f"    Warning: {warning}: {exc}")
        return False


def ensure_schema(spark: Any) -> None:
    if USE_THREE_PART_IDENTIFIER and LAKEHOUSE:
        namespace = ".".join([quote_spark_identifier(LAKEHOUSE), quote_spark_identifier(SCHEMA_NAME)])
        if try_sql(spark, f"CREATE SCHEMA IF NOT EXISTS {namespace}", f"Could not ensure schema {namespace}"):
            return

    namespace = quote_spark_identifier(SCHEMA_NAME)
    try_sql(spark, f"CREATE SCHEMA IF NOT EXISTS {namespace}", f"Could not ensure schema {namespace}")


def ensure_catalog_registration(spark: Any, table: TableDef) -> None:
    identifier = table_identifier(table.target_name)
    path = table_path(table.target_name)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {identifier} USING DELTA LOCATION '{path}'")


def schema_signature(schema: Any) -> list[tuple[str, str, bool]]:
    return [
        (field.name.upper(), field.dataType.simpleString(), bool(field.nullable))
        for field in schema.fields
    ]


def reset_schema_mismatch_log() -> None:
    SCHEMA_MISMATCHES.clear()
    DDL_RUN_ISSUES.clear()


def issue_base_record(table: TableDef, issue_type: str) -> dict[str, Any]:
    return {
        "logged_at_utc": datetime.now(timezone.utc).isoformat(),
        "issue_type": issue_type,
        "source_schema": table.source_schema,
        "source_table": table.source_name,
        "target_schema": SCHEMA_NAME,
        "target_table": table.target_name,
        "target_path": table_path(table.target_name),
    }


def record_schema_mismatch(table: TableDef, issues: list[str], expected_schema: Any, actual_schema: Any) -> None:
    record = issue_base_record(table, "schema_mismatch")
    record.update(
        {
            "issues": issues,
            "expected_schema": [
                {
                    "name": name,
                    "data_type": data_type,
                    "nullable": nullable,
                }
                for name, data_type, nullable in schema_signature(expected_schema)
            ],
            "actual_schema": [
                {
                    "name": name,
                    "data_type": data_type,
                    "nullable": nullable,
                }
                for name, data_type, nullable in schema_signature(actual_schema)
            ],
        }
    )
    SCHEMA_MISMATCHES.append(record)
    DDL_RUN_ISSUES.append(record)


def record_table_error(table: TableDef, operation: str, error: Exception) -> None:
    record = issue_base_record(table, f"{operation}_error")
    record.update(
        {
            "operation": operation,
            "error_type": type(error).__name__,
            "error_message": str(error),
        }
    )
    DDL_RUN_ISSUES.append(record)


def print_schema_mismatch_summary() -> None:
    if not DDL_RUN_ISSUES:
        print("DDL issue summary: no issues found.")
        return

    print("=" * 72)
    print("DDL issue summary")
    print("=" * 72)
    print(f"Issues logged: {len(DDL_RUN_ISSUES)}")
    for issue in DDL_RUN_ISSUES:
        print(f" - {issue['issue_type']}: {issue['target_schema']}.{issue['target_table']}")
        if "issues" in issue:
            for detail in issue["issues"]:
                print(f"   * {detail}")
        elif "error_message" in issue:
            print(f"   * {issue['error_type']}: {issue['error_message']}")


def build_schema_mismatch_report_lines() -> list[str]:
    if not SCHEMA_MISMATCHES:
        return ["Schema mismatch report: no mismatches found."]

    lines = [
        f"Schema mismatch report - generated {datetime.now(timezone.utc).isoformat()}",
        "=" * 72,
        "",
    ]
    for mismatch in SCHEMA_MISMATCHES:
        lines.append(f"{mismatch['target_schema']}.{mismatch['target_table']}")
        for issue in mismatch["issues"]:
            lines.append(f"  - {issue}")
        lines.append("")
    return lines


def write_schema_mismatch_log(spark: Any | None = None) -> None:
    if not WRITE_SCHEMA_MISMATCH_LOG:
        return

    if not SCHEMA_MISMATCHES:
        print("Schema mismatch log not written; no schema mismatches found.")
        return

    lines = build_schema_mismatch_report_lines()
    path = SCHEMA_MISMATCH_LOG_PATH
    if is_remote_path(path):
        if spark is None:
            spark = build_spark_session()
        spark.createDataFrame([(line,) for line in lines], ["value"]).coalesce(1).write.mode(
            "overwrite"
        ).text(path)
        print(f"Schema mismatch log written to: {path}")
        return

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Schema mismatch log written to: {output_path}")


def compare_schema(expected_schema: Any, actual_schema: Any, table: TableDef) -> list[str]:
    expected = schema_signature(expected_schema)
    actual = schema_signature(actual_schema)
    issues: list[str] = []

    expected_names = [name for name, _, _ in expected]
    actual_names = [name for name, _, _ in actual]

    missing = [name for name in expected_names if name not in actual_names]
    extra = [name for name in actual_names if name not in expected_names]
    if missing:
        issues.append(f"missing columns: {missing}")
    if extra:
        issues.append(f"extra columns: {extra}")

    common_expected_order = [name for name in expected_names if name in actual_names]
    common_actual_order = sorted(
        (name for name in actual_names if name in expected_names),
        key=actual_names.index,
    )
    if common_expected_order != common_actual_order:
        issues.append(
            f"column order mismatch: expected {common_expected_order}, found {common_actual_order}"
        )

    actual_by_name = {name: (data_type, nullable) for name, data_type, nullable in actual}
    for name, expected_type, expected_nullable in expected:
        actual_type_nullable = actual_by_name.get(name)
        if actual_type_nullable is None:
            continue
        actual_type, actual_nullable = actual_type_nullable
        if actual_type != expected_type:
            issues.append(f"{name} type expected {expected_type}, found {actual_type}")
        if actual_nullable and not expected_nullable:
            issues.append(f"{name} expected NOT NULL but catalog is nullable")

    if issues:
        record_schema_mismatch(table, issues, expected_schema, actual_schema)
        message = f"{table.target_name} schema mismatch: " + "; ".join(issues)
        if FAIL_ON_SCHEMA_MISMATCH:
            raise ValueError(message)
        print(f"    Warning: {message}")

    return issues


def write_constraint_metadata(spark: Any, table: TableDef) -> None:
    if not WRITE_CONSTRAINT_METADATA:
        return

    metadata = {
        "oracle.source_schema": table.source_schema or "",
        "oracle.source_table": table.source_name,
        "oracle.primary_key": json.dumps(asdict(table.primary_key) if table.primary_key else {}),
        "oracle.unique_keys": json.dumps([asdict(item) for item in table.unique_keys]),
        "oracle.foreign_keys": json.dumps([asdict(item) for item in table.foreign_keys]),
        "oracle.check_constraints": json.dumps([asdict(item) for item in table.checks]),
        "oracle.not_null_columns": json.dumps(table.not_null_columns),
        "oracle.defaults": json.dumps(table.defaults),
        "oracle.unique_indexes": json.dumps([asdict(item) for item in table.unique_indexes]),
    }

    property_sql = ", ".join(
        f"'{key}' = '{value.replace(chr(39), chr(39) + chr(39))}'"
        for key, value in metadata.items()
    )
    identifier = table_identifier(table.target_name)
    try_sql(
        spark,
        f"ALTER TABLE {identifier} SET TBLPROPERTIES ({property_sql})",
        f"Could not write Oracle metadata properties for {identifier}",
    )


def create_or_register_table(spark: Any, table: TableDef) -> None:
    path = table_path(table.target_name)
    expected_schema = build_spark_schema(table)

    exists = table_exists_at_path(spark, path)
    if exists and not REPLACE_EXISTING_TABLES:
        print(f">>> {table.target_name}: Delta table already exists; validating/registering.")
        ensure_catalog_registration(spark, table)
        actual_schema = spark.read.format("delta").load(path).schema
        compare_schema(expected_schema, actual_schema, table)
        write_constraint_metadata(spark, table)
        return

    mode = "overwrite" if REPLACE_EXISTING_TABLES else "errorifexists"
    print(f">>> {table.target_name}: creating Delta table at {path}")
    empty_df = spark.createDataFrame([], expected_schema)
    (
        empty_df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .save(path)
    )
    ensure_catalog_registration(spark, table)
    write_constraint_metadata(spark, table)


# =============================================================================
# SECTION 5 - VALIDATION
# =============================================================================

def count_where(df: Any, condition: Any) -> int:
    return df.filter(condition).count()


def validate_schema(spark: Any, table: TableDef) -> None:
    if not RUN_SCHEMA_VALIDATION:
        return

    path = table_path(table.target_name)
    expected_schema = build_spark_schema(table)
    df = spark.read.format("delta").load(path)
    issues = compare_schema(expected_schema, df.schema, table)
    if issues:
        print(f"    Schema validation completed with warnings for {table.target_name}; row count: {df.count()}")
    else:
        print(f"    Schema validation passed for {table.target_name}; row count: {df.count()}")


def validate_not_nulls(df: Any, table: TableDef) -> list[str]:
    from pyspark.sql import functions as F

    issues: list[str] = []
    for column_name in table.not_null_columns:
        violations = count_where(df, F.col(column_name).isNull())
        if violations:
            issues.append(f"{column_name} has {violations} NULL rows")
    return issues


def validate_primary_key(df: Any, table: TableDef) -> list[str]:
    from pyspark.sql import functions as F

    if not table.primary_key:
        return []

    issues: list[str] = []
    key_columns = table.primary_key.columns

    null_condition = None
    for key_column in key_columns:
        condition = F.col(key_column).isNull()
        null_condition = condition if null_condition is None else (null_condition | condition)

    if null_condition is not None:
        null_count = count_where(df, null_condition)
        if null_count:
            issues.append(f"primary key {key_columns} has {null_count} rows with NULL key parts")

    duplicate_count = (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    if duplicate_count:
        issues.append(f"primary key {key_columns} has {duplicate_count} duplicate key groups")

    return issues


def simple_check_violation_count(df: Any, check: CheckConstraintDef) -> int | None:
    from pyspark.sql import functions as F

    not_null_match = re.match(
        r'^\s*(?:"?([A-Za-z_][\w$#]*)"?)\s+IS\s+NOT\s+NULL\s*$',
        check.expression,
        re.IGNORECASE,
    )
    if not_null_match:
        return count_where(df, F.col(not_null_match.group(1).upper()).isNull())

    return None


def validate_checks(df: Any, table: TableDef) -> list[str]:
    issues: list[str] = []
    for check in table.checks:
        violations = simple_check_violation_count(df, check)
        if violations is None:
            print(
                f"    Check {check.name or '<unnamed>'} on {table.target_name} "
                f"requires manual validation: {check.expression}"
            )
            continue
        if violations:
            issues.append(
                f"check {check.name or check.expression} has {violations} violating rows"
            )
    return issues


def validate_foreign_keys(spark: Any, df: Any, table: TableDef) -> list[str]:
    from pyspark.sql import functions as F

    issues: list[str] = []
    for foreign_key in table.foreign_keys:
        if not foreign_key.ref_columns or len(foreign_key.columns) != len(foreign_key.ref_columns):
            print(
                f"    Foreign key {foreign_key.name or '<unnamed>'} skipped: "
                "referenced columns were not explicit in the Oracle DDL."
            )
            continue

        ref_target_name = TABLE_NAME_OVERRIDES.get(
            foreign_key.ref_table,
            to_snake_identifier(foreign_key.ref_table),
        )
        ref_path = table_path(ref_target_name)
        if not table_exists_at_path(spark, ref_path):
            print(
                f"    Foreign key {foreign_key.name or '<unnamed>'} skipped: "
                f"referenced table {ref_target_name} was not found."
            )
            continue

        ref_df = spark.read.format("delta").load(ref_path)
        local_alias = "local"
        ref_alias = "ref"
        join_condition = None
        for local_column, ref_column in zip(foreign_key.columns, foreign_key.ref_columns):
            condition = F.col(f"{local_alias}.{local_column}") == F.col(f"{ref_alias}.{ref_column}")
            join_condition = condition if join_condition is None else (join_condition & condition)

        non_null_condition = None
        for local_column in foreign_key.columns:
            condition = F.col(local_column).isNotNull()
            non_null_condition = condition if non_null_condition is None else (non_null_condition & condition)

        candidate_df = df.filter(non_null_condition) if non_null_condition is not None else df
        orphan_count = (
            candidate_df.alias(local_alias)
            .join(ref_df.alias(ref_alias), join_condition, "left_anti")
            .count()
        )
        if orphan_count:
            issues.append(
                f"foreign key {foreign_key.name or foreign_key.columns} has {orphan_count} orphan rows"
            )

    return issues


def validate_constraints(spark: Any, table: TableDef) -> None:
    if not RUN_CONSTRAINT_VALIDATION:
        return

    path = table_path(table.target_name)
    df = spark.read.format("delta").load(path)
    row_count = df.count()
    if row_count == 0:
        print(f"    Constraint validation skipped for {table.target_name}; table is empty.")
        return

    issues = []
    issues.extend(validate_not_nulls(df, table))
    issues.extend(validate_primary_key(df, table))
    issues.extend(validate_checks(df, table))
    issues.extend(validate_foreign_keys(spark, df, table))

    if issues:
        raise ValueError(f"{table.target_name} constraint validation failed: {'; '.join(issues)}")

    print(f"    Constraint validation passed for {table.target_name}.")


# =============================================================================
# SECTION 6 - ORCHESTRATION
# =============================================================================

def summarize_tables(tables: Iterable[TableDef]) -> None:
    table_list = list(tables)
    print("=" * 72)
    print("Oracle DDL parse summary")
    print("=" * 72)
    print(f"Tables parsed       : {len(table_list)}")
    print(f"Primary keys        : {sum(1 for table in table_list if table.primary_key)}")
    print(f"Foreign keys        : {sum(len(table.foreign_keys) for table in table_list)}")
    print(f"Check constraints   : {sum(len(table.checks) for table in table_list)}")
    print(f"Unique indexes      : {sum(len(table.unique_indexes) for table in table_list)}")

    for table in table_list:
        print(
            f" - {table.source_schema or '<default>'}.{table.source_name} -> "
            f"{SCHEMA_NAME}.{table.target_name} "
            f"({len(table.columns)} columns)"
        )


def parse_tables_from_oracle_ddl(ddl_text: str, summarize: bool = True) -> list[TableDef]:
    tables = parse_oracle_ddl(ddl_text)
    if summarize:
        summarize_tables(tables)
    return tables


def load_and_parse_tables(
    input_path: str | None = None,
    spark: Any | None = None,
    summarize: bool = True,
) -> list[TableDef]:
    ddl_text = load_ddl_text(input_path, spark=spark)
    return parse_tables_from_oracle_ddl(ddl_text, summarize=summarize)


def create_tables(tables: Iterable[TableDef], spark: Any | None = None) -> Any:
    if spark is None:
        spark = build_spark_session()

    ensure_schema(spark)
    for table in tables:
        try:
            create_or_register_table(spark, table)
        except Exception as exc:
            record_table_error(table, "create", exc)
            if not CONTINUE_ON_TABLE_ERROR:
                raise
            print(f"    Warning: create/register failed for {table.target_name}: {exc}")

    return spark


def validate_tables(tables: Iterable[TableDef], spark: Any | None = None) -> Any:
    if spark is None:
        spark = build_spark_session()

    for table in tables:
        try:
            validate_schema(spark, table)
            validate_constraints(spark, table)
        except Exception as exc:
            record_table_error(table, "validate", exc)
            if not CONTINUE_ON_TABLE_ERROR:
                raise
            print(f"    Warning: validation failed for {table.target_name}: {exc}")

    return spark


def run_ddl_section(
    section: str = RUN_SECTION,
    input_path: str | None = None,
    spark: Any | None = None,
    dry_run: bool = False,
) -> list[TableDef]:
    reset_schema_mismatch_log()
    normalized_section = section.strip().lower()
    if dry_run:
        normalized_section = "parse"

    if normalized_section not in {"parse", "create", "validate", "all"}:
        raise ValueError("section must be one of: parse, create, validate, all")

    resolved_input_path = input_path or DDL_INPUT_PATH
    needs_spark_for_input = (
        not ORACLE_DDL_TEXT.strip()
        and resolved_input_path
        and (is_remote_path(resolved_input_path) or not Path(resolved_input_path).exists())
    )
    if spark is None and needs_spark_for_input:
        spark = build_spark_session()

    tables = load_and_parse_tables(input_path=input_path, spark=spark, summarize=True)

    if normalized_section == "parse":
        return tables

    if spark is None:
        spark = build_spark_session()

    try:
        if normalized_section in {"create", "all"}:
            spark = create_tables(tables, spark=spark)

        if normalized_section in {"validate", "all"}:
            validate_tables(tables, spark=spark)
    finally:
        print_schema_mismatch_summary()
        write_schema_mismatch_log(spark=spark)

    print("=" * 72)
    print(f"Gold dbo section '{normalized_section}' completed for {len(tables)} tables.")
    print("=" * 72)
    return tables


def create_tables_from_oracle_ddl(ddl_text: str, spark: Any | None = None, dry_run: bool = False) -> list[TableDef]:
    reset_schema_mismatch_log()
    tables = parse_tables_from_oracle_ddl(ddl_text, summarize=True)

    if dry_run:
        return tables

    if spark is None:
        spark = build_spark_session()

    try:
        spark = create_tables(tables, spark=spark)
        validate_tables(tables, spark=spark)
    finally:
        print_schema_mismatch_summary()
        write_schema_mismatch_log(spark=spark)

    print("=" * 72)
    print(f"Gold dbo table creation completed for {len(tables)} tables.")
    print("=" * 72)
    return tables


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create Fabric Gold.dbo Delta tables from Oracle DDL.")
    parser.add_argument("--ddl-file", default=None, help="Oracle DDL input file path.")
    parser.add_argument("--dry-run", action="store_true", help="Parse and summarize without requiring Spark.")
    parser.add_argument(
        "--section",
        choices=["parse", "create", "validate", "all"],
        default=RUN_SECTION,
        help="Run only one notebook-style section.",
    )
    parser.add_argument("--replace", action="store_true", help="Overwrite existing Delta tables.")
    parser.add_argument(
        "--skip-constraint-validation",
        action="store_true",
        help="Create/register tables without scanning data for constraint violations.",
    )
    args, _ = parser.parse_known_args()
    return args


def main() -> None:
    global REPLACE_EXISTING_TABLES
    global RUN_CONSTRAINT_VALIDATION

    args = parse_args()
    if args.replace:
        REPLACE_EXISTING_TABLES = True
    if args.skip_constraint_validation:
        RUN_CONSTRAINT_VALIDATION = False

    run_ddl_section(section=args.section, input_path=args.ddl_file, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
