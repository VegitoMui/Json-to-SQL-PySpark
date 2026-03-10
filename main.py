from src.json_reader import read_json
from src.generators.sql_generator import generate_sql
from src.generators.pyspark_generator import generate_pyspark


def main():

    config = read_json("config/sample.json")

    print("\nGenerated SQL:\n")
    print(generate_sql(config))

    print("\nGenerated PySpark Code:\n")
    print(generate_pyspark(config))


if __name__ == "__main__":
    main()