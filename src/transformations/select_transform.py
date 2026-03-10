def handle_select_sql(t):
    return ", ".join(t["columns"])


def handle_select_pyspark(t):
    cols = ", ".join([f'"{c}"' for c in t["columns"]])
    return f"df = df.select({cols})\n"