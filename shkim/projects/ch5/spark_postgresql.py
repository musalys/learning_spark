from pyspark.sql import SparkSession, Row

spark = (SparkSession.builder
                     .appName("SparkSQLExampleApp")
                     .getOrCreate())

# 로드 함수를 사용하여 JDBC 소스로부터 데이터를 로드
jdbcDF1 = (spark.read.format("jdbc")
                     .option("url", "jdbc:postgresql://localhost:5432/dev")
                     .option("dbtable", "people")
                     .option("user", "learning_spark")
                     .option("password", "spark1234")
                     .load())

print(jdbcDF1)
jdbcDF1.show()

# 연결 속성을 사용하여 데이터를 로드
# jdbcDF2 = (spark.read.jdbc("jdbc:postgresql:[DBSERVER]", "[SCHEMA].[TABLENAME]", properties={"user": "[USERNAME]", "password": "[PASSWORD]"}))

# 쓰기 방버1 : 저장 함수를 사용하여 데이터를 저장
data_set = [ Row(name="shkim", age=35),
             Row(name="yj", age=20) ]

df_test = spark.createDataFrame(data_set)
df_test.show()

(df_test.write.format("jdbc")
              .option("url", "jdbc:postgresql://localhost:5432/dev")
              .option("dbtable", "people")
              .option("user", "learning_spark")
              .option("password", "spark1234")
              .save(mode="append"))

# 쓰기 방법2 : jdbc 함수를 사용하여 저장
#(jdbcDF2.write.jdbc("jdbc:postgresql:[DBSERVER]", "[SCHEMA].[TABLENAME]", properties={"user": "[USERNAME]", "password": "[PASSWORD]"}))
