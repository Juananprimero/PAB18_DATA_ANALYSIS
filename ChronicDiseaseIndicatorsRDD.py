import sys

from pyspark import SparkConf, SparkContext
import time

def main(file_name: str) -> None:
    spark_conf = SparkConf()
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    start_computing_time = time.time()

    airports = spark_context \
        .textFile(file_name)\
        .map(lambda line: line.split(","))\
        .filter(lambda list : list[8] == "\"ES\"")\
        .map(lambda list: (list[2], 1))\
        .reduceByKey(lambda x, y: x + y)\
        .sortBy(lambda pair: pair[1])

    result = airports\
        .collect()

    for pair in result:
        print(pair)

    total_computing_time = time.time() - start_computing_time
    print("Computing time: ", str(total_computing_time))

    spark_context.stop()


if __name__ == "__main__":
    """
    Python program that uses Apache Spark to find 
    """

    if len(sys.argv) != 2:
        print("Usage: spark-submit ChronicDiseaseIndicatorsRDD.py <file>", file=sys.stderr)
        exit(-1)

    main(sys.argv[1])