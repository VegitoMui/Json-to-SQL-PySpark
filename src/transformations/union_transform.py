def handle_union_sql(t):

    table = t["table"]
    union_type = t.get("union_type", "UNION").upper()

    return f"{union_type} SELECT * FROM {table}"


def handle_union_pyspark(t):

    table = t["table"]
    union_type = t.get("union_type", "union").lower()

    code = f'{table}_df = spark.table("{table}")\n'

    if union_type == "union":
        code += f"df = df.union({table}_df)\n"
    else:
        code += f"df = df.unionByName({table}_df)\n"

    return code