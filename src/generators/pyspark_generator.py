from src.transformations.registry import PYSPARK_TRANSFORM_REGISTRY


def generate_pyspark(config):

    table = config["source"]["table"]
    transformations = config["transformations"]

    code = (
        "from pyspark.sql.functions import when, col, row_number, rank, dense_rank, lag, lead, avg, max, count\n"
        "from pyspark.sql.window import Window\n\n"
    )

    code += f'df = spark.table("{table}")\n'

    for t in transformations:

        t_type = t["type"]
        handler = PYSPARK_TRANSFORM_REGISTRY.get(t_type)

        if not handler:
            raise ValueError(f"Unsupported transformation: {t_type}")

        result = handler(t)

        # some handlers may return None
        if result:
            code += result

    return code