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


## 지연에 대한 표시 해보
