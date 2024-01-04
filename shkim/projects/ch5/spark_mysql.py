# ./spark-3.1.1-bin-hadoop2.7/bin/spark-submit --driver-class-path /Users/magae/project/data/github.com/magaeTube/learning_spark/jdbc/mysql-connector-java-8.0.30.jar shkim/projects/ch5/spark_mysql.py
from pyspark.sql import SparkSession, Row

spark = (SparkSession.builder
                     .appName("SparkSQLExampleApp")
                     .getOrCreate())

# 로드 함수를 사용하여 JDBC 소스로부터 데이터를 로드
jdbcDF = (spark.read.format("jdbc")
                     .option("url", "jdbc:mysql://localhost:3306/dev")
                     .option("dbtable", "people")
                     .option("user", "learning_spark")
                     .option("password", "spark1234")
                     .load())

jdbcDF.show()

# 쓰기 방법1 : 저장 함수를 사용하여 데이터를 저장
data_set = [ Row(name="shkim", age=35),
             Row(name="yj", age=20) ]

df_test = spark.createDataFrame(data_set)
df_test.show()

(df_test.write.format("jdbc")
              .option("url", "jdbc:mysql://localhost:3306/dev")
              .option("dbtable", "people")
              .option("user", "learning_spark")
              .option("password", "spark1234")
              .save(mode="append"))