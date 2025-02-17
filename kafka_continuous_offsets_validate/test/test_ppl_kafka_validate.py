import unittest

from ppl_kafka_validate import run_transform, say
from helpers import runQuery

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import SparkContext
#import time

class SparkETLTests(unittest.TestCase):

    def setUp(self):

        conf = (SparkConf()
         .setAppName("Java Spark SQL example")
         .setMaster("local[*]")
         .set("spark.driver.host","localhost")
         .set("spark.sql.session.timeZone", "UTC"))

        self.sc = SparkContext(conf=conf)
        self.spark = SparkSession.builder.getOrCreate()

    def tearDown(self):
        self.spark.stop()

    def test_transform(self):

        res = run_transform(self.spark,
                  self.spark.read.option("header", "true").csv('test/data/kafka_messages.csv'),
                  "2024-12-28",
                  "10")

        runQuery(self.spark,"select * from kafka_messages", 20);
        runQuery(self.spark,"select * from t_stg_events", 20);
        runQuery(self.spark,"select * from t_stg_event_continuous_ranges");

        say("RESULT");
        res.show(100)

        print("\n\n ========= Unit Tests ========= \n\n")

        expected_row_count = 3
        self.assertEqual(res.count(), expected_row_count)
        say("Final dataset row count = " + str(expected_row_count) + " validation passed");

if __name__ == '__main__':
    unittest.main()