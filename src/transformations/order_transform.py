def handle_order_sql(t):

    cols = t["columns"]
    order_parts = []

    for col in cols:

        lower = col.lower()

        if " desc" in lower or " asc" in lower:
            order_parts.append(col)
        else:
            order_parts.append(f"{col} ASC")

    return " ORDER BY " + ", ".join(order_parts)

def handle_order_pyspark(t):

    cols = t["columns"]

    order_parts = []

    for col_expr in cols:

        col_name, direction = col_expr.split()

        if direction.lower() == "desc":
            order_parts.append(f'col("{col_name}").desc()')
        else:
            order_parts.append(f'col("{col_name}").asc()')

    return f"df = df.orderBy({', '.join(order_parts)})\n"