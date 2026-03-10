def handle_subquery_sql(t):

    from src.generators.sql_generator import generate_sql

    alias = t["alias"]

    subquery_sql = generate_sql({
        "source": t["source"],
        "transformations": t["transformations"]
    })

    return f"({subquery_sql}) {alias}"

def handle_subquery_pyspark(t):

    alias = t["alias"]
    code = "sub_df = df\n"
    code += f"df = sub_df.alias('{alias}')\n"

    return code