def handle_window_sql(t):

    func = t["function"].upper()
    alias = t["alias"]

    partition_clause = ""
    order_clause = ""

    if "partition_by" in t:
        partition_cols = ", ".join(t["partition_by"])
        partition_clause = f"PARTITION BY {partition_cols}"

    if "order_by" in t:
        order_cols = ", ".join(t["order_by"])
        order_clause = f" ORDER BY {order_cols}"

    window_expr = f"{func}() OVER ({partition_clause}{order_clause}) AS {alias}"

    return window_expr


def handle_window_pyspark(t):

    func = t["function"]
    partition = t.get("partition_by", [])
    order = t.get("order_by", [])
    alias = t["alias"]

    partition_str = ", ".join([f'"{p}"' for p in partition])

    order_parts = []
    for o in order:
        col_name, direction = o.split()
        if direction.lower() == "desc":
            order_parts.append(f'col("{col_name}").desc()')
        else:
            order_parts.append(f'col("{col_name}").asc()')

    order_str = ", ".join(order_parts)

    code = f'window_spec = Window.partitionBy({partition_str}).orderBy({order_str})\n'
    code += f'df = df.withColumn("{alias}", {func}().over(window_spec))\n'

    return code