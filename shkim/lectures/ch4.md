## 스파크 SQL 특징
* 정형화 API가 엔진으로 제공됨.
* 다양한 정형 데이터를읽거나 쓸 수 있음
* 외부 BI 데이터 소스나 RDBMS 데이터를 JDBC/ODBC 커넥터를 사용하여 쿼리할 수 있음.
* 대화형 셸 제공
* ANSI SQL 2003 및 HiveQL 지원


# 스파크 애플리케이션에서 스파크 SQL 사용
* SQL 쿼리를 실행하기 위해서는 `spark.sql("쿼리")`로 사용할 수 있음.
* 결과는 데이터 프레임으로 변환함


## 기본 쿼리 예제
* 스칼라
```scala
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder
                        .appName("SparkSQLExampleApp")
                        .getOrCreate()
                        
// 데이터세트 경로
val csvFile = "../../LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"

// 읽고 임시뷰를 생성
// 스키마 추론
val df = spark.read.format("csv")
                   .option("inferSchema", "true")
                   .option("header", "true")
                   .load(csvFile)
                   
// 임시뷰 생성
df.createOrReplaceTempView("us_delay_flights_tbl")                                 
```

* 파이썬
```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
                     .appName("SparkSQLExampleApp")
                     .getOrCreate())

csvFile = "../../LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"

df = (spark.read.format("csv")
               .option("inferSchema", "true")
               .option("header", "true")
               .load(csvFile))
                   
df.createOrReplaceTempView("us_delay_flights_tbl")         
```


## 비행거리가 1000마일 이상인 모든 항공편
```python
spark.sql("""
    SELECT distance, origin, destination
      FROM us_delay_flights_tbl
     WHERE distance > 1000
     ORDER BY distance DESC
""").show(10)
```


## 샌프란시스코와 시카고 간 2시간 이상 지연이 있었던 항공편
```python
spark.sql("""
    SELECT date, delay, origin, destination
      FROM us_delay_flights_tbl
     WHERE delay > 120
       AND origin = 'SFO'
       AND destination = 'ORD'
     ORDER BY delay DESC
""").show(10)
```

결과에 date가 int형으로 되어  있음. csv 파일을 읽을 때 추론을 해서 그런지 date 컬럼이 int로 되어 있음.

```scala
// 스키마 정의
val schema = "date STRING, delay INT, origin STRING, destination STRING"

// 읽고 임시뷰를 생성
// 스키마 추론
// 쉘에서 한줄로 실행해야 함.
val df = spark.read.format("csv")
                   .option("header", "true")
                   .schema(schema)
                   .load(csvFile)
                   
// 임시뷰 생성
df.createOrReplaceTempView("us_delay_flights_tbl")               
```
스키마 정의 후 다시 테이블 생성


## 지연에 대한 표시 해보기
```python
spark.sql("""
    SELECT delay, origin, destination,
           CASE WHEN delay > 360 THEN 'Very Long Delays'
                WHEN delay >= 120 AND delay <= 360 THEN 'Long Delays'
                WHEN delay >= 60 AND delay < 120 THEn 'Short Delays'
                WHEN delay > 0  AND delay < 60 THEN 'Tolerable Delays'
                WHEN delay = 0 THEN 'No Delays'
                ELSE 'Early'
           END AS Flight_Delays
      FROM us_delay_flights_tbl
     ORDER BY delay DESC
""").show(10)
```


## 데이터 프레임으로 해보기
```python
# 비행거리가 1000마일 이상인 항공편
from pyspark.sql.functions import col, desc
(df.select("distance", "origin", "destination")
   .where(col("distance") > 1000)
   .orderBy(desc("distance"))).show(10)

# 샌프란시스코와 시카고 간 2시간 이상 지연이 있던 항공편
(df.select("date", "delay", "origin", "destination")
   .where(col("delay") > 120)
   .where(col("origin") == 'SFO')
   .where(col("destination") == 'ORD')
   .orderBy(desc("delay"))).show(10)

# 지연에 대한 표시
from pyspark.sql.functions import when
(df.select("delay", "origin", "destination")
   .withColumn("Flight_Delays", when(col("delay") > "360", "Very Long Delays")
                               .when((col("delay") >= "120") & (col("delay") <= "360"), "Long Delays")
                               .when((col("delay") >= "60") & (col("delay") < "120"), "Short Delays")
                               .when((col("delay") > "0") & (col("delay") < "60"), "Tolerable Delays")
                               .when(col("delay") == "0", "No Delays")
                               .otherwise("Early"))
   .orderBy("origin", desc("delay"))).show(10)
```


# SQL 테이블과 뷰 
* 스파크는 각 테이블과 스키마 정보 등 메타데이터를 기본적으로 /user/hive/warehouse에 있는 하이브 메타스토어를 사용하여 저장한다.

## 관리형/비관리형 테이블
* 관리형 : 메타데이터와 파일 저장소의 데이터 모두 관리
* 비관리형 : 메타데이터만 관리
* DROP 명령어 실행시 관리형은 메타데이터와 데이터 모두 지우지만 비관리형은 메타데이터만 지움

## SQL 데이터베이스와 테이블 생성하기
*  스파크는 기본적으로 default 데이터베이스 안에 테이블을 생성함.

```python
# 데이터베이스 생성
spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")

# 관리형 테이블 생성
spark.sql("""
    CREATE TABLE managed_us_delay_flights_tbl
    (date STRING, delay INT, distance INT, origin STRING, destination STRING)
""")

# 관리형 테이블 생성 (데이터프레임 API)
csv_file  = "../../LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
flights_df=  spark.read.csv(csv_file, schema=schema)
flights_df.write.saveAsTable("managed_us_delay_flights_tbl")

# 비관리형 테이블 생성
spark.sql("""
    CREATE TABLE us_delay_flights_tbl (
        date STRING,
        delay INT,
        distance INT,
        origin STRING,
        destination STRING
    )
    USING csv OPTIONS (PATH, '../../LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/departuredelays.csv')
""")

# 비관리형 테이블 생성 (데이터프레임 API)
(flights_df.write
           .option("path", "/tmp/data/us_flights_delay")
           .saveAsTable("us_delay_flights_tbl"))  
```

# 뷰 생성하기
* 전역(모든 SparkSession) / 세션 범위 (단일 SparkSession) 가능
```python
df_sfo = spark.sql("""
    SELECT date, delay, origin, destination
      FROM us_delay_flights_tbl
     WHERE origin = 'SFO'
""")

df_jfk = spark.sql("""
    SELECT date, delay, origin, destination
      FROM us_delay_flights_tbl
     WHERE origin = 'JFK'
""")

df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

spark.read.table("us_origin_airport_JFK_tmp_view")

# DROP
spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")
spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")
```

