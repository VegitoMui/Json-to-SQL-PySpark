def handle_case_sql(t):

    case_sql = "CASE "

    for c in t["cases"]:
        case_sql += f"WHEN {c['when']} THEN {c['then']} "

    case_sql += f"ELSE {t['else']} END AS {t['alias']}"

    return case_sql


def handle_case_pyspark(t):

    alias = t["alias"]
    cases = t["cases"]

    def wrap_condition(cond):
        return cond.replace("salary", 'col("salary")')

    case_code = f'when({wrap_condition(cases[0]["when"])}, {cases[0]["then"]})'

    for c in cases[1:]:
        case_code += f'.when({wrap_condition(c["when"])}, {c["then"]})'

    case_code += f'.otherwise({t["else"]})'

    return f'df = df.withColumn("{alias}", {case_code})\n'