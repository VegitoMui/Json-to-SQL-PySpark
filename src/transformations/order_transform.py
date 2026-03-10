def handle_order_sql(t):

    cols = ", ".join(t["columns"])
    order = t.get("order", "asc").upper()

    return f" ORDER BY {cols} {order}"


def handle_order_pyspark(t):

    cols = t["columns"]
    order = t.get("order", "asc")

    if order.lower() == "desc":
        return f'df = df.orderBy(*{cols}, ascending=False)\n'

    return f'df = df.orderBy(*{cols})\n'