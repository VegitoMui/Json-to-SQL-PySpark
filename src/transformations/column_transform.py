def handle_column_transform_sql(t):

    expressions = []

    for col in t["columns"]:

        name = col["name"]

        if "cast" in col:
            expr = f"CAST({name} AS {col['cast'].upper()})"
        else:
            expr = name

        if "alias" in col:
            expr += f" AS {col['alias']}"

        expressions.append(expr)

    return ", ".join(expressions)


def handle_column_transform_pyspark(t):

    code = ""

    for col_info in t["columns"]:

        name = col_info["name"]

        if "cast" in col_info:
            expr = f'col("{name}").cast("{col_info["cast"]}")'
        else:
            expr = f'col("{name}")'

        new_name = col_info.get("alias", name)

        code += f'df = df.withColumn("{new_name}", {expr})\n'

    return code

    return code