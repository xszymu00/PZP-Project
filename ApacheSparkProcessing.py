import time

import findspark
import os
from ctypes import sizeof
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, desc, lower, regexp_replace, length, col, trim, last
from operator import add

from pyspark.sql import SparkSession

def RunSpark():
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
    os.environ["SPARK_HOME"] = "spark-3.2.2-bin-hadoop2.7/"
    findspark.init()

    spark = SparkSession.builder.appName("PZP-Project").getOrCreate()

    start = time.time()
    dataDF = spark.read.text("data.txt").select("value")
    stopWordsDF = spark.read.text("stop_words.txt").select("value")
    stopWordsDF = stopWordsDF.select(regexp_replace(stopWordsDF.value, '\W', '').alias("words"))
    stopWordsDF = stopWordsDF.select(lower(stopWordsDF.words).alias("words"))
    stopWordsDF = stopWordsDF.select(trim(stopWordsDF.words).alias("words"))

    dataDF = dataDF.select(split(dataDF.value, "\W").alias("split"))
    wordsDF = dataDF.select(explode(dataDF.split).alias('words'))
    wordsDF = wordsDF.select(trim(wordsDF.words).alias("words"))
    wordsDF = wordsDF.select(lower(wordsDF.words).alias("words"))
    wordsDF = wordsDF.filter(wordsDF.words != "")
    wordsDF = wordsDF.filter(length(col("words")) >= 4)
    wordsDF = wordsDF.filter(length(col("words")) <= 8)
    wordsDF = wordsDF.join(stopWordsDF, "words", "leftanti")

    wordCountDF = wordsDF.groupBy('words').count()
    wordCountDF = wordCountDF.sort("count", ascending=False)
    stop = time.time()


    mostFrequentWord = wordCountDF.first()
    leastFrequentWord = wordCountDF.tail(1)
    sumOfWords = wordCountDF.groupBy().sum().first()

    print("---APACHE SPARK---")
    print(f"Most frequent word is: '{mostFrequentWord[0]}' and occurred {mostFrequentWord[1]} times.")
    print(f"Least frequent word occurred {leastFrequentWord[0][1]} times and the example is '{leastFrequentWord[0][0]}'.")
    print(f"Total number of filtered words is: {sumOfWords[0]}.")
    print(f"Total processing time was: {round(stop - start, 7) * 1000} ms.\n")

RunSpark()