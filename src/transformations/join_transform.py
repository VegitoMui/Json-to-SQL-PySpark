def handle_join_sql(t, base_table):

    join_type = t.get("join_type", "inner").upper()
    join_table = t["table"]

    if "on" in t:
        condition = t["on"]
    else:
        condition = f"{base_table}.{t['on_left']} = {join_table}.{t['on_right']}"

    return f" {join_type} JOIN {join_table} ON {condition}"


def handle_join_pyspark(t):

    join_table = t["table"]
    join_type = t.get("join_type", "inner")

    code = f'{join_table}_df = spark.table("{join_table}")\n'

    if "on" in t:
        condition = t["on"]
        code += f'df = df.join({join_table}_df, "{condition}", "{join_type}")\n'

    else:
        left = t["on_left"]
        right = t["on_right"]

        code += (
            f'df = df.join({join_table}_df, '
            f'df["{left}"] == {join_table}_df["{right}"], "{join_type}")\n'
        )

    return code