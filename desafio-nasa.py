#!/bin/bash Spark2.4, Python 3

# prepare environment

# import libraries
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from operator import add

# Spark configurations
conf = (SparkConf()
        .setMaster("local")
        .setAppName("Semantix")
        .set("spark.executor.memory", "4g"))

# initialize SparkContext
sc = SparkContext(conf=conf)

# load files
# load file access_log_Jul95
jul = sc.textFile("access_log_Jul95")
jul = jul.cache()

# load file access_log_Aug95
aug= sc.textFile("access_log_Aug95")
aug = aug.cache()

# 1. Numero de hosts unicos

def count_unique_host(rdd, period):
    try:
        hosts = rdd.flatMap(lambda i: i.split(" ")[0]).distinct().count()  
    except ValueError as e:
        print(e)
    return print("Number of unique hosts on {}: {}.".format(period, hosts))

count_unique_host(jul, "july")
count_unique_host(aug, "august")

# 2. O total de erros 404

def count_error_404(rdd, period):
    try:
        def url_404(line):
            try:
                url = line.split(" ")[-2]
                if url == "404":
                    return True
            except:
                pass
            return False
        count = rdd.filter(url_404).cache()
        count_404 = count.count()
    except ValueError as e:
        print(e)
    return print("Total number of 404 erros on {}: {}.".format(period, count_404))

count_error_404(jul, "july")
count_error_404(aug, "august")

# 3. Os 5 URLs que mais causaram erro 404

def url_404(rdd):
    try:
        url = rdd.split(" ")[-2]
        if url == "404":
            return True
    except ValueError as e:
        print(e)
    return False

jul_404 = jul.filter(url_404).cache()
aug_404 = aug.filter(url_404).cache()

def top5_url_error_404(rdd_url_404):
    try:
        url_404 = rdd_url_404.map(lambda line: line.split('"')[1].split(' ')[1])
        count_404 = url_404.map(lambda i: (i, 1)).reduceByKey(add)
        top_5 = count_404.sortBy(lambda i: -i[1]).take(5)
        print("Top 5 url with most frequent 404 errors:")
        for url_404, count_404 in top_5:
            print("{}: {}".format(url_404, count_404))
        return top_5
    except ValueError as e:
        print(e)
    return False

top5_url_error_404(jul_404)
top5_url_error_404(aug_404)

# 4. Quantidade de erros 404 por dia

def count_error_404_daily(rdd_url_404, period):
    try:
        days = rdd_url_404.map(lambda i: i.split("[")[1].split(":")[0])
        counts = days.map(lambda day: (day, 1)).reduceByKey(add).collect()
        print("Number of errors 404 per day on the month of {}:".format(period))
        for day, count in counts:
            print("{}: {}".format(day, count))
        return counts
    except ValueError as e:
        print(e)
    return False

count_error_404_daily(jul_404, "july")
count_error_404_daily(aug_404, "august")

# 5. O total de bytes retornados

def size_file_bytes(rdd, period):
    try:
        def count_byte(line):
            try:
                count = int(line.split(" ")[-1])
                if count < 0:
                    raise ValueError()
                return count
            except:
                return 0
        count = rdd.map(count_byte).reduce(lambda x, y: x + y)
        return print("Number of bytes of {} file: {}".format(period, count))
    except ValueError as e:
        print(e)
    return ""

size_file_bytes(jul, "july")
size_file_bytes(aug, "august")


sc.stop()
