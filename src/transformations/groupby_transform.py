def handle_groupby_sql(t):

    group_cols = ", ".join(t["columns"])

    agg_expressions = []

    for agg in t["aggregations"]:

        func = agg["function"].upper()
        col = agg["column"]
        alias = agg["alias"]

        agg_expressions.append(f"{func}({col}) AS {alias}")

    agg_sql = ", ".join(agg_expressions)

    select_cols = f"{group_cols}, {agg_sql}"

    group_clause = f" GROUP BY {group_cols}"

    return select_cols, group_clause


def handle_groupby_pyspark(t):

    cols = t["columns"]

    agg_list = []

    for agg in t["aggregations"]:

        func = agg["function"]
        col = agg["column"]
        alias = agg["alias"]

        agg_list.append(f'{func}("{col}").alias("{alias}")')

    agg_code = ", ".join(agg_list)

    return f'df = df.groupBy({cols}).agg({agg_code})\n'