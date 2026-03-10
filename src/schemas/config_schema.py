from pydantic import BaseModel, Field, ValidationError
from typing import List, Optional, Union, Literal, Annotated


# ---------- SOURCE ----------

class Source(BaseModel):
    table: str


# ---------- SELECT ----------

class SelectTransform(BaseModel):
    type: Literal["select"]
    columns: List[str]


# ---------- FILTER ----------

class FilterTransform(BaseModel):
    type: Literal["filter"]
    condition: str


# ---------- JOIN ----------

class JoinTransform(BaseModel):
    type: Literal["join"]
    table: str
    on_left: str
    on_right: str
    join_type: Optional[str] = "inner"


# ---------- ORDER BY ----------

class OrderTransform(BaseModel):
    type: Literal["order_by"]
    columns: List[str]
    order: Optional[str] = "asc"


# ---------- LIMIT ----------

class LimitTransform(BaseModel):
    type: Literal["limit"]
    value: int


# ---------- CASE WHEN ----------

class CaseCondition(BaseModel):
    when: str
    then: str


class CaseTransform(BaseModel):
    type: Literal["case_when"]
    alias: str
    cases: List[CaseCondition]
    else_: str = Field(alias="else")


# ---------- COLUMN TRANSFORM ----------

class ColumnTransformItem(BaseModel):
    name: str
    alias: Optional[str] = None
    cast: Optional[str] = None


class ColumnTransform(BaseModel):
    type: Literal["column_transform"]
    columns: List[ColumnTransformItem]


# ---------- WINDOW ----------

class WindowTransform(BaseModel):
    type: Literal["window"]
    function: str
    partition_by: Optional[List[str]] = None
    order_by: Optional[List[str]] = None
    alias: str


# ---------- GROUP BY ----------

class Aggregation(BaseModel):
    column: str
    function: str
    alias: str

# ---------- DISTINCT ----------

class DistinctTransform(BaseModel):
    type: Literal["distinct"]

# ---------- UNION ----------

class UnionTransform(BaseModel):
    type: Literal["union"]
    table: str
    union_type: Optional[str] = "union"

# ---------- SUBQUERY ----------

class SubqueryTransform(BaseModel):
    type: Literal["subquery"]
    alias: str
    source: Source
    transformations: List[dict]



class GroupByTransform(BaseModel):
    type: Literal["groupby"]
    columns: List[str]
    aggregations: List[Aggregation]


# ---------- DISCRIMINATED UNION ----------

Transformation = Annotated[
    Union[
        SelectTransform,
        FilterTransform,
        JoinTransform,
        OrderTransform,
        LimitTransform,
        CaseTransform,
        ColumnTransform,
        WindowTransform,
        GroupByTransform,
        DistinctTransform,
        UnionTransform,
        SubqueryTransform
    ],
    Field(discriminator="type"),
]

# ---------- MAIN CONFIG ----------

class ConfigSchema(BaseModel):
    source: Source
    transformations: List[Transformation]


def validate_config(config: dict):

    try:
        ConfigSchema(**config)

    except ValidationError as e:

        errors = e.errors()

        print("\nConfiguration validation failed\n")
        print(f"{len(errors)} error(s) found:\n")

        for err in errors:

            loc = err["loc"]
            path = ""

            for part in loc:
                if isinstance(part, int):
                    path += f"[{part}]"
                else:
                    if path:
                        path += f".{part}"
                    else:
                        path = part

            print(f"• {path}")
            print(f"  → {err['msg']}\n")

        raise SystemExit(1)

    return config