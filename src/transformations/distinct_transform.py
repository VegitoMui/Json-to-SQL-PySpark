def handle_distinct_sql(t):
    return "DISTINCT"


def handle_distinct_pyspark(t):
    return "df = df.distinct()\n"