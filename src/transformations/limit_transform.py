def handle_limit_sql(t):
    return f" LIMIT {t['value']}"


def handle_limit_pyspark(t):
    return f"df = df.limit({t['value']})\n"