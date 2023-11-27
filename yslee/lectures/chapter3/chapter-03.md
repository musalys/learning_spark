## 아파치 스파크의 정형화 API (Apache Sparks Structured APIs)
### TL;DR
#### What is Sparks Structured APIs?
> High-level APIs for Spark's DataFrame and DataSet 

#### Spark RDD
1. dependency
	1. inputs
	2. how it made of
	3. flexibility and reproducibility
2. partition(data regionality included)
	1. parallelism
3. Compute function: Partition => `Iterator[T]`
	1. not enough info about types
	2. incapable of compute optimizing
	3. can't apply compression techniques and only use serialization of objects

#### Establishing Spark's Structure
스파크 2.x는 스파크 구조 확립을 위한 핵심 개념들을 도입
  - 데이터 분석에 사용되는 일상적인 패턴 사용 (필터링, 선택, 집합연산, 집계, 평균 그룹화)
  - 위를 통해 지원 언어로 API사용이 가능해져, 효율적인 플랜 작성이 가능해짐
  - SQL의 테이블이나 스프레드시트처럼 지원하는 정형화 데이터타입을 이용 Tabular data를 사용 가능

#### Core benefits of Spark's Structure

- 표현성
- 단순성
- 구성용이성
- 일관성 / 통일성

Without spark's structure
```python

dataRDD = sc.parallelize([("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)])
agesRDD = (dataRDD
		  .map(lambda x: (x[0], (x[1], 1)))
		  .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
		  .map(lambda x: (x[0], x[1][0]/x[1][1]))
		  )
```

With spark's structure
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
# Make DataFrame from SparkSession
spark = (SparkSession
		.builder
		.appName("AuthorsAges")
		.getOrCreate())
data_df = spark.createDataFrame([("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)])
avg_df = data_df.groupBy("name").agg(avg("age"))
avg_df.show()
```

  사용자가 무엇을 원하는지 API를 이용해 스파크에게 무엇을 할지를 알려주므로 표현력이 높고 간단하다. 따라서, 스파크는 이런 쿼리를 파악해서 사용자의 의도를 이해할 수 있기 때문에 효과적인 실행을 위해 연산들을 최적화하거나 적절하게 재배열할 수 있다.

  위 방향성은 언어를 통틀어 일관성을 갖고 있어서, 스칼라/자바/R/SQL 코드도 같은 일을 하면서도 형태도 비슷한 코드를 가지고 있다.

#### DataFrame API

##### Spark Basic Data Types in Scala

__데이터 타입__|__스칼라 할당 값__|__초기 생성 API__
-- | -- | --
ByteType|Byte|DataTypes.ByteType
ShortType|Short|DataTypes.ShortType
IntegerType|Integer|DataTypes.IntegerType
LongType|Long|DataTypes.LongType
FloatType|Float|DataTypes.FloatType
DoubleType|Double|DataTypes.DoubleType
StringType|String|DataTypes.StringType
BooleanType|Boolean|DataTypes.BooleanType
DecimalType|java.math.BigDecimal|DecimalType

##### Spark Basic Data Types in Python

__데이터 타입__|__파이썬 할당 값__|__초기 생성 API__
-- | -- | --
ByteType|int|DataTypes.ByteType
ShortType|int|DataTypes.ShortType
IntegerType|int|DataTypes.IntegerType
LongType|int|DataTypes.LongType
FloatType|float|DataTypes.FloatType
DoubleType|float|DataTypes.DoubleType
StringType|string|DataTypes.StringType
BooleanType|bool|DataTypes.BooleanType
DecimalType|decimal.Decimal|DecimalType


##### Spark structure types

##### Schema and DataFrame

```scala
// Use types
import org.apache.spark.sql.types._
val schema = StructType(Array(StructField("authour", StringType, false),
							 StructField("title", StringType, false),
							 StructField("pages", IntegerType, false)))
// Use DDL
val schema = "authour STRING, title STRING, pages INT"
```

```python
# Use types
from pyspark.sql.types import *
schema = StructType([StructField("author", StringType(), False),
					 StructField("title", StringType(), False),
					 StructField("pages", IntegerType(), False)])
# Use DDL
schema = "author STRING, title STRING, pages INT"
```


52Page 예제
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

schema = """
Id INT, First STRING, Last STRING, Url STRING, Published STRING, Hits INT, Campaigns ARRAY<STRING>
"""

#create our data
data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "LinkedIn"]],
[2, "Brooke","Wenig","https://tinyurl.2", "5/5/2018", 8908, ["twitter", "LinkedIn"]],
[3, "Denny", "Lee", "https://tinyurl.3","6/7/2019",7659, ["web", "twitter", "FB", "LinkedIn"]],
[4, "Tathagata", "Das","https://tinyurl.4", "5/12/2018", 10568, ["twitter", "FB"]],
[5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web", "twitter", "FB", "LinkedIn"]],
[6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"]]]

# main program
if __name__ == "__main__":

#create a SparkSession
spark = (SparkSession
.builder
.appName("Example-3_6")
.getOrCreate())

# create a DataFrame using the schema defined above
blogs_df = spark.createDataFrame(data, schema)

# show the DataFrame; it should reflect our table above
blogs_df.show()

# print the schema used by Spark to process the DataFrame
print(blogs_df.printSchema())
```

※ `spark-3.2.4-bin-hadoop2.7` 버전에서 위 예제의 schema column 정의할 때 컬럼 구분자로 `'` 사용하면 error 발생

```python
Traceback (most recent call last):
  File "/Users/iyunseog/Desktop/spark_study/LearningSparkV2/chapter3/py/src/Example-3_6.py", line 34, in <module>
    blogs_df = spark.createDataFrame(data, schema)
  File "/Users/iyunseog/Desktop/spark_study/spark-3.2.4-bin-hadoop2.7/python/lib/pyspark.zip/pyspark/sql/session.py", line 661, in createDataFrame
  File "/Users/iyunseog/Desktop/spark_study/spark-3.2.4-bin-hadoop2.7/python/lib/pyspark.zip/pyspark/sql/types.py", line 843, in _parse_datatype_string
  File "/Users/iyunseog/Desktop/spark_study/spark-3.2.4-bin-hadoop2.7/python/lib/pyspark.zip/pyspark/sql/types.py", line 833, in _parse_datatype_string
  File "/Users/iyunseog/Desktop/spark_study/spark-3.2.4-bin-hadoop2.7/python/lib/pyspark.zip/pyspark/sql/types.py", line 825, in from_ddl_schema
  File "/Users/iyunseog/Desktop/spark_study/spark-3.2.4-bin-hadoop2.7/python/lib/py4j-0.10.9.5-src.zip/py4j/java_gateway.py", line 1321, in __call__
  File "/Users/iyunseog/Desktop/spark_study/spark-3.2.4-bin-hadoop2.7/python/lib/pyspark.zip/pyspark/sql/utils.py", line 117, in deco
pyspark.sql.utils.ParseException: 
extraneous input ''Id'' expecting {'ADD', 'AFTER', 'ALL', 'ALTER', 'ANALYZE', 'AND', 'ANTI', 'ANY', 'ARCHIVE', 'ARRAY', 'AS', 'ASC', 'AT', 'AUTHORIZATION', 'BETWEEN', 'BOTH', 'BUCKET', 'BUCKETS', 'BY', 'CACHE', 'CASCADE', 'CASE', 'CAST', 'CHANGE', 'CHECK', 'CLEAR', 'CLUSTER', 'CLUSTERED', 'CODEGEN', 'COLLATE', 'COLLECTION', 'COLUMN', 'COLUMNS', 'COMMENT', 'COMMIT', 'COMPACT', 'COMPACTIONS', 'COMPUTE', 'CONCATENATE', 'CONSTRAINT', 'COST', 'CREATE', 'CROSS', 'CUBE', 'CURRENT', 'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'CURRENT_USER', 'DAY', 'DATA', 'DATABASE', DATABASES, 'DBPROPERTIES', 'DEFINED', 'DELETE', 'DELIMITED', 'DESC', 'DESCRIBE', 'DFS', 'DIRECTORIES', 'DIRECTORY', 'DISTINCT', 'DISTRIBUTE', 'DIV', 'DROP', 'ELSE', 'END', 'ESCAPE', 'ESCAPED', 'EXCEPT', 'EXCHANGE', 'EXISTS', 'EXPLAIN', 'EXPORT', 'EXTENDED', 'EXTERNAL', 'EXTRACT', 'FALSE', 'FETCH', 'FIELDS', 'FILTER', 'FILEFORMAT', 'FIRST', 'FOLLOWING', 'FOR', 'FOREIGN', 'FORMAT', 'FORMATTED', 'FROM', 'FULL', 'FUNCTION', 'FUNCTIONS', 'GLOBAL', 'GRANT', 'GROUP', 'GROUPING', 'HAVING', 'HOUR', 'IF', 'IGNORE', 'IMPORT', 'IN', 'INDEX', 'INDEXES', 'INNER', 'INPATH', 'INPUTFORMAT', 'INSERT', 'INTERSECT', 'INTERVAL', 'INTO', 'IS', 'ITEMS', 'JOIN', 'KEYS', 'LAST', 'LATERAL', 'LAZY', 'LEADING', 'LEFT', 'LIKE', 'LIMIT', 'LINES', 'LIST', 'LOAD', 'LOCAL', 'LOCATION', 'LOCK', 'LOCKS', 'LOGICAL', 'MACRO', 'MAP', 'MATCHED', 'MERGE', 'MINUTE', 'MONTH', 'MSCK', 'NAMESPACE', 'NAMESPACES', 'NATURAL', 'NO', NOT, 'NULL', 'NULLS', 'OF', 'ON', 'ONLY', 'OPTION', 'OPTIONS', 'OR', 'ORDER', 'OUT', 'OUTER', 'OUTPUTFORMAT', 'OVER', 'OVERLAPS', 'OVERLAY', 'OVERWRITE', 'PARTITION', 'PARTITIONED', 'PARTITIONS', 'PERCENT', 'PIVOT', 'PLACING', 'POSITION', 'PRECEDING', 'PRIMARY', 'PRINCIPALS', 'PROPERTIES', 'PURGE', 'QUERY', 'RANGE', 'RECORDREADER', 'RECORDWRITER', 'RECOVER', 'REDUCE', 'REFERENCES', 'REFRESH', 'RENAME', 'REPAIR', 'REPLACE', 'RESET', 'RESPECT', 'RESTRICT', 'REVOKE', 'RIGHT', RLIKE, 'ROLE', 'ROLES', 'ROLLBACK', 'ROLLUP', 'ROW', 'ROWS', 'SECOND', 'SCHEMA', 'SELECT', 'SEMI', 'SEPARATED', 'SERDE', 'SERDEPROPERTIES', 'SESSION_USER', 'SET', 'MINUS', 'SETS', 'SHOW', 'SKEWED', 'SOME', 'SORT', 'SORTED', 'START', 'STATISTICS', 'STORED', 'STRATIFY', 'STRUCT', 'SUBSTR', 'SUBSTRING', 'SYNC', 'TABLE', 'TABLES', 'TABLESAMPLE', 'TBLPROPERTIES', TEMPORARY, 'TERMINATED', 'THEN', 'TIME', 'TO', 'TOUCH', 'TRAILING', 'TRANSACTION', 'TRANSACTIONS', 'TRANSFORM', 'TRIM', 'TRUE', 'TRUNCATE', 'TRY_CAST', 'TYPE', 'UNARCHIVE', 'UNBOUNDED', 'UNCACHE', 'UNION', 'UNIQUE', 'UNKNOWN', 'UNLOCK', 'UNSET', 'UPDATE', 'USE', 'USER', 'USING', 'VALUES', 'VIEW', 'VIEWS', 'WHEN', 'WHERE', 'WINDOW', 'WITH', 'YEAR', 'ZONE', IDENTIFIER, BACKQUOTED_IDENTIFIER}(line 2, pos 0)

== SQL ==

'Id' INT, 'First' STRING, 'Last' STRING, 'Url' STRING, 'Published' STRING, 'Hits' INT, 'Campaigns' ARRAY<STRING>
^^^
```

##### Columns & Expressions

- 컬럼은 공개 메소드를 가진 객체로 표현
- 논리식이나 수학 표현식을 컬럼에 사용할 수도 있음
- expr(), col()

```scala
// Calculation column with expr
blogsDF.select(expr("Hits * 2")).show(2)

// Calculation column with col
blogsDF.select(col("Hits") * 2).show(2)

// Assign new column "Big Hitters" with expr
blogsDF.withColumn("Big Hitters", (expr("Hits > 10000"))).show()

// Same result with "SELECT Hits FROM blogsDF"
blogsDF.select(expr("Hits")).show(2)
blogsDF.select(col("Hits")).show(2)
blogsDF.select("Hits").show(2)

// Creates new column AuthorsId and show it
blogsDF.withColumn(
	"AuthoursId", (concat(expr("First"), expr("Last"), expr("Id"))))
	.select(col("AuthorsId"))
	.show(4)

// Sort descending with "Id" column
blogsDF.sort(col("Id").desc).show()
blogsDF.sort($"Id".desc).show()
```

데이터 프레임의 Column 객체는 단독으로 존재할 수 없다. 각 Column은 한 레코드의 Row의 일부분이며 모든 Row가 합쳐져서 하나의 데이터 프레임을 구성한다.

##### Row

스파크에서 하나의 행은 일반적으로 하나 이상의 칼럼을 갖고 있는 Row 객체로 표현된다. Row는 스파크의 객체이고 순서가 있는 필드 집합 객체이므로 각 필드를 0부터 시작하는 인덱스로 접근한다.

```python
# spark-3.2.4-bin-hadoop2.7

>>> from pyspark.sql import Row
>>> blog_row = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015", ["twitter", "LinkedIn"])
>>> blog_row[1]
'Reynold'
>>> rows = [Row("Matei Zaharia", "CA"), Row("Reynold Xin", "CA")]
>>> authors_df = spark.createDataFrame(rows, ["Authors", "State"]
... )
>>> authors_df.show()
+-------------+-----+                                                           
|      Authors|State|
+-------------+-----+
|Matei Zaharia|   CA|
|  Reynold Xin|   CA|
+-------------+-----+
```

대부분의 경우 파일들은 규모가 크기 때문에 스키마를 미리 지정해 사용하는 것이 데이터 프레임 작성에 훨씬 더 빠르고 효율적인 방법이다.

#### Frequently used DataFrame jobs

##### IO
- DataFrameReader
- DataFrameWriter
##### Available Formats
1. JSON
2. CSV
3. Parquet
4. Avro
5. ORC
6. txt
7. Other Sources(Databases, Kafka...)

##### DataFrameReader & DataFrameWriter

__*DataFrameReader*__

샌프란시스코 소방서 호출 데이터는 28개의 컬럼과 4,380,660개 이상의 레코드가 있기 때문에, 스키마를 미리 지정해 주는 것이 훨씬 효과적이다.

> 스키마를 미리 지정하고 싶지 않다면, 스파크가 적은 비용으로 샘플링해서 스키마를 추론할 수 있게 할 수는 있다. 예를 들면 다음처럼 samplingRatio 옵션을 적용하는 것이 가능하다

```python
# pyspark 3.2.4
>>> sampleDF = spark.read.option("samplingRatio", 0.001).option("header", True).csv("/Users/iyunseog/Desktop/spark_study/LearningSparkV2/chapter3/data/sf-fire-calls.csv")
>>> sampleDF.show(20)
```

__*DataFrameWriter*__

데이터 프레임을 외부 데이터 소스에 원하는 포맷으로 쓰려면 *DataFrameWriter* 인터페이스를 사용할 수 있다.
기본 포맷: parquet
압축: snappy

데이터 저장은 아래처럼 저장한다.
```python
# 파이썬에서 파케이로 저장
parquet_path = ...
sampleDF.write.format("parquet").save(parquet_path)
```

혹은 하이브 메타스토어에 메타데이터로 등록되는 테이블로 저장할 수 있다
```python
parquet_table = ... # 테이블 이름
sampleDF.write.format("parquet").saveAsTable(parquet_table)
```

##### Transformations & Actions

프로젝션과 필터
spark에서의
프로젝션: select()
필터: filter() & where()

63p 파이썬 예제 1
```python
# pyspark 3.2.4
>>> from pyspark.sql.functions import *
>>> few_sampleDF = sampleDF.select("IncidentNumber", "AvailableDtTm", "CallType").where(col("CallType") != "Medical Incident")
>>> few_sampleDF.show(5, False)

# Out
+--------------+----------------------+--------------+
|IncidentNumber|AvailableDtTm         |CallType      |
+--------------+----------------------+--------------+
|2003235       |01/11/2002 01:51:44 AM|Structure Fire|
|2003250       |01/11/2002 04:16:46 AM|Vehicle Fire  |
|2003259       |01/11/2002 06:01:58 AM|Alarms        |
|2003279       |01/11/2002 08:03:26 AM|Structure Fire|
|2003301       |01/11/2002 09:46:44 AM|Alarms        |
+--------------+----------------------+--------------+
only showing top 5 rows
```

63p 파이썬 예제 2
```python
# pyspark 3.2.4
>>> sampleDF.select("CallType").where(col("CallType").isNotNull()).agg(countDistinct("CallType").alias("DistinctCallTypes")).show()

# Out
+-----------------+                                                             
|DistinctCallTypes|
+-----------------+
|               30|
+-----------------+
```

책의 결과와 조금 다름. 이부분은 확인 필요.

##### Column names change & add & remove

parquet 포맷에 컬럼이름 공백 포함 금지

```python
# pyspark 3.2.4
>>> new_sampleDF = sampleDF.withColumnRenamed("Delay", "ResponseDelayedMins")
>>> new_sampleDF.select("ResponseDelayedMins").where(col("ResponseDelayedMins") > 5).show(5, False)

# Out
+-------------------+
|ResponseDelayedMins|
+-------------------+
|6.25               |
|7.25               |
|11.916667          |
|8.633333           |
|95.28333           |
+-------------------+
only showing top 5 rows
```

> 데이터 프레임 변형은 변경 불가 방식으로 동작하므로 withColumnRenamed()로 컬럼 이름을 변경할 때는, 기존 컬럼 이름을 갖고 있는 원본을 유지한 채로 컬럼 이름이 변경된 새로운 데이터 프레임을 받아오게 된다

날짜/시간 변환
`to_timestamp()`나 `to_date()` 같은 이름의 함수들이 존재한다.

```python
# pyspark 3.2.4
>>> fire_ts_df = (new_sampleDF
...     .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy")) \
...     .drop("CallDate") \
...     .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy")) \
...     .drop("WatchDate") \
...     .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a")) \
...     .drop("AvailableDtTm"))
>>> fire_ts_df.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show(5, False)

# Out
+-------------------+-------------------+-------------------+
|IncidentDate       |OnWatchDate        |AvailableDtTS      |
+-------------------+-------------------+-------------------+
|2002-01-11 00:00:00|2002-01-10 00:00:00|2002-01-11 01:51:44|
|2002-01-11 00:00:00|2002-01-10 00:00:00|2002-01-11 03:01:18|
|2002-01-11 00:00:00|2002-01-10 00:00:00|2002-01-11 02:39:50|
|2002-01-11 00:00:00|2002-01-10 00:00:00|2002-01-11 04:16:46|
|2002-01-11 00:00:00|2002-01-10 00:00:00|2002-01-11 06:01:58|
+-------------------+-------------------+-------------------+
only showing top 5 rows
```

```python
# pyspark 3.2.4
>>> fire_ts_df.select(year("IncidentDate")).distinct().orderBy(year("IncidentDate")).show()

# Out
+------------------+                                                            
|year(IncidentDate)|
+------------------+
|              2000|
|              2001|
|              2002|
|              2003|
|              2004|
|              2005|
|              2006|
|              2007|
|              2008|
|              2009|
|              2010|
|              2011|
|              2012|
|              2013|
|              2014|
|              2015|
|              2016|
|              2017|
|              2018|
+------------------+
```

집계연산

`groupBy()`, `orderBy()`, `count()`와 같이 데이터 프레임에서 쓰는 일부 트랜스포메이션과 액션은 컬럼 이름으로 집계해서 각각 개수를 세어주는 기능을 제공한다.

> 자주 혹은 반복적으로 질의할 필요가 있는 규모가 큰 데이터 프레임에서는 캐싱을 해서 이득을 얻을 수 있다.

```python
# pyspark 3.2.4
>>> fire_ts_df.select("CallType").where(col("CallType").isNotNull()).groupBy("CallType") \
...     .count() \
...     .orderBy("count", ascending=False) \
...     .show(10, truncate=False)

# Out
+-------------------------------+------+                                        
|CallType                       |count |
+-------------------------------+------+
|Medical Incident               |113794|
|Structure Fire                 |23319 |
|Alarms                         |19406 |
|Traffic Collision              |7013  |
|Citizen Assist / Service Call  |2524  |
|Other                          |2166  |
|Outside Fire                   |2094  |
|Vehicle Fire                   |854   |
|Gas Leak (Natural and LP Gases)|764   |
|Water Rescue                   |755   |
+-------------------------------+------+
only showing top 10 rows
```

> 데이터 프레임 API는 collect() 함수를 제공하지만 극단적으로 큰 데이터 프레임에서는 메모리 부족 예외(OOM)를 발생시킬 수 있기 때문에 자원도 많이 쓰고 위험하다. 드라이버에 결과 숫자 하나만 전달하는 count()와는 달리 collect()는 전체 데이터 프레임 혹은 데이터세트의 모든 Row 객체 모음을 되돌려준다. 몇개의 Row 결과만 보고 싶다면 최초 n 개의 Row 객체만 되돌려 주는 take(n) 함수를 쓰는 것이 훨씬 나을 것 이다.

##### Other DataFrame functions

`min() max() sum() avg()` 등을 지원.

```python
# pyspark 3.2.4

>>> import pyspark.sql.functions as F
>>> fire_ts_df.select(F.sum("NumAlarms"), F.avg("ResponseDelayedMins"), F.min("ResponseDelayedMins"), F.max("ResponseDelayedMins")).show()
```

데이터 과학 작업에서 일반적으로 쓰이는 고수준 함수
`stat(), describe(), correlation(), covariance(), sampleBy(), approxiQuantile(), frequentItems()` 등의 API 문서 참고.
#### Dataset API

스파크 2.0에서는 개발자들이 한 종류의 API만 알면 되게 하기 위해 데이터 프레임과 데이터세트 API를 유사한 인터페이스를 갖도록 정형화 API로 일원화 했다.

데이터세트는 아래와 같은 특성을 모두 가진다
1. 정적 타입(*typed*) API `Dataset[T]`
2. 동적 타입 (*untyped*) API `Dataset[Row]`

개념적으로 스칼라의 데이터 프레임은 공용 객체 모음인 `Dataset[Row]`의 다른 이름(alias)이며, `Row`는 서로 다른 타입의 값을 저장할 수 있는 포괄적 JVM 객체이다.

데이터세트는 스칼라에서 엄격하게 타입이 정해진 JVM 객체의 집합이며 이는 자바에서 클래스로 볼 수 있다.

##### 정적 타입 객체, 동적 타입 객체, 포괄적인 Row

__언어__ | __동적__ | __정적__ | 추상화 객체
-- | -- | -- | --
스칼라 | O | O | `Dataset[T]`, `DataFrame(Dataset[Row]의 앨리어싱)`
자바 | X | O | `Dataset<T>`
파이썬 | O | X | `DataFrame`
R | O | X | `DataFrame`

Row는 포괄적 객체 타입으로 (배열처럼) 인덱스를 사용하여 접근할 수 있으며 다양한 타입의 값들을 담을 수 있다. 내부적으로 스파크는 표 3-2, 표 3-3에 있는 타입들로 바꿔서 쓸 수 있게 Row 객체를 변환한다. 그리고 Row 객체에 공개되어 있는 게터(*getter*)류 함수들에 인덱스를 사용해 개별 필드에 접근할 수 있다.

반면, 정적 객체들은 JVM에서 실제 자바클래스나 스칼라 클래스가 된다. 그러므로 데이터세트의 각 아이템들은 곧바로 하나의 JVM 객체가 되어 쓸 수 있다.

