from pyspark.sql import SparkSession

def create_spark_session():
    return (
        SparkSession.builder
        .appName("Appodeal Assesment")
        # .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

def read_json_files(spark, paths):
    return spark.read.option("multiLine", True).json(paths)

