def handle_subquery_sql(t):

    from src.generators.sql_generator import generate_sql

    alias = t["alias"]

    subquery_sql = generate_sql({
        "source": t["source"],
        "transformations": t["transformations"]
    })

    return f"({subquery_sql}) {alias}"

def handle_subquery_pyspark(t):

    from src.generators.pyspark_generator import generate_pyspark

    alias = t["alias"]

    sub_config = {
        "source": t["source"],
        "transformations": t["transformations"]
    }

    inner_code = generate_pyspark(sub_config)

    code = "\n# ----- SUBQUERY START -----\n"
    code += inner_code
    code += f'df = df.alias("{alias}")\n'
    code += "# ----- SUBQUERY END -----\n\n"

    return code