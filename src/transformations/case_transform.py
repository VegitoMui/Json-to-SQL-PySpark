def handle_case_sql(t):

    case_sql = "CASE "

    for c in t["cases"]:
        case_sql += f"WHEN {c['when']} THEN {c['then']} "

    case_sql += f"ELSE {t['else']} END AS {t['alias']}"

    return case_sql


def handle_case_pyspark(t):

    cases = t["cases"]
    else_val = t["else"]
    alias = t["alias"]

    code = f'df = df.withColumn("{alias}", '

    first = True

    for c in cases:

        cond = c["when"]

        cond = cond.replace("salary_double", 'col("salary_double")')

        if first:
            code += f'when({cond}, {c["then"]})'
            first = False
        else:
            code += f'.when({cond}, {c["then"]})'

    code += f'.otherwise({else_val}))\n'

    return code