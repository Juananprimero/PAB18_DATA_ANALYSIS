import time, sys

from pyspark.sql import SparkSession

def main(file_name) -> None:

    spark_session = SparkSession \
        .builder \
        .getOrCreate()

    logger = spark_session._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    start_computing_time = time.time()

    data_frame = spark_session \
        .read \
        .format("csv") \
        .options(header='true', inferschema='true') \
        .load(file_name)

    data_frame.printSchema()
    data_frame.show()

    data_frame \
        .filter(data_frame["YearStart"] == 2013) \
        .select("Topic", "Question") \
        .distinct() \
        .orderBy("Topic") \
        .groupBy("Topic") \
        .count() \
        .show()

    total_computing_time = time.time() - start_computing_time
    print("Computing time: ", str(total_computing_time))


if __name__ == '__main__':
    """
    Python program that uses Apache Spark to read a .csv file and shows a table using the dataframe API.
    """
    if len(sys.argv) != 2:
        print("Usage: spark-submit ChronicDiseaseIndicatorsDF.py <file>", file=sys.stderr)
        exit(-1)

    main(sys.argv[1])
