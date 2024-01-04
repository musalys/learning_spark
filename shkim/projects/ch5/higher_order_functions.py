from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = (SparkSession.builder
                     .appName("SparkSQLExampleApp")
                     .getOrCreate())

schema = StructType([StructField("celsius", ArrayType(IntegerType()))])

t_list = [[35, 36, 32, 30, 40, 42, 38]], [[31, 32, 34, 55, 56]]
t_c = spark.createDataFrame(t_list, schema)
t_c.createOrReplaceTempView("tC")

t_c.show()

# transform()
# 입력 배열의 각 요소에 함수를 적용하여 배열을 생성한다.
spark.sql("""
    SELECT celsius
         , transform(celsius, t -> ((t * 9) div 5) + 32) AS fahrenheit
      FROM tC
""").show()

# filter()
# 입력한 배열의 요소 중 부울 함수가 참인 요소만으로 구성된 배열을 생성
spark.sql("""
    SELECT celsius
         , filter(celsius, t -> t > 38) AS high
      FROM tC
""").show()


# exists()
# 입력한 배열의 요소 중 불린 함수를 만족시키는 것이 존재하면 참을 반환함
spark.sql("""
    SELECT celsius
         , exists(celsius, t -> t = 38) as threshold
      FROM tC
""").show()

# reduce()
# function<B, T, B>를 사용하여 요소를 버퍼 B에 병합하고 최종 버퍼에 마무리 function<B, R>을 적용하여 배열의 요소를 단일값으로 줄인다.
spark.sql("""
    SELECT celsius
         , reduce(
               celsius,
               cast(0 as bigint),
               (t, acc) -> t + acc,
               acc -> (acc div size(celsius) * 9 div 5) + 32
           ) as avgFahrenheit
      FROM tC
""").show()