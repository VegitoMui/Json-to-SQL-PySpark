from src.transformations.registry import SQL_TRANSFORM_REGISTRY


def generate_sql(config):

    table = config["source"]["table"]
    transformations = config["transformations"]

    base_columns = "*"
    computed_columns = []

    join_clause = ""
    where_clause = ""
    group_clause = ""
    order_clause = ""
    limit_clause = ""

    distinct_flag = False
    union_clauses = []

    for t in transformations:

        t_type = t["type"]
        handler = SQL_TRANSFORM_REGISTRY.get(t_type)

        if not handler:
            raise ValueError(f"Unsupported transformation: {t_type}")

        if t_type == "select":
            base_columns = handler(t)

        elif t_type == "distinct":
            distinct_flag = True

        elif t_type == "join":
            join_clause += handler(t, table)

        elif t_type == "filter":
            where_clause = handler(t)

        elif t_type == "groupby":
            base_columns, group_clause = handler(t)

        elif t_type == "order_by":
            order_clause = handler(t)

        elif t_type == "limit":
            limit_clause = handler(t)

        elif t_type == "union":
            union_clauses.append(handler(t))

        elif t_type == "subquery":
            # recursive SQL generation
            subquery_sql = generate_sql({
                "source": t["source"],
                "transformations": t["transformations"]
            })

            table = f"({subquery_sql}) {t['alias']}"

        else:
            computed_columns.append(handler(t))

    if computed_columns:
        select_cols = base_columns + ", " + ", ".join(computed_columns)
    else:
        select_cols = base_columns

    distinct_sql = "DISTINCT " if distinct_flag else ""

    query = (
        f"SELECT {distinct_sql}{select_cols} "
        f"FROM {table}"
        f"{join_clause}"
        f"{where_clause}"
        f"{group_clause}"
        f"{order_clause}"
        f"{limit_clause}"
    )

    # apply unions
    for u in union_clauses:
        query += f"\n{u}"

    return query