def handle_filter_sql(t):
    return f" WHERE {t['condition']}"


def handle_filter_pyspark(t):
    return f'df = df.filter("{t["condition"]}")\n'