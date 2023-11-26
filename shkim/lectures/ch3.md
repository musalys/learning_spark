# 3장. 아파치 스파크의 정형화 API

## 스파크: RDD의 아래에는 무엇이 있는가
* RDD는 스파크에서 가장 기본적인 추상적 부분으로 세 가지의 핵심 특성이 있음
  * 의존성 (Dependency): 어떤 입력과 현재의 RDD가 만들어지는 지에 대해 스파크에게 알려줌
  * 파티션 (지역성 정보 포함): 스파크에게 작업을 나눠서 파티션별로 병렬 연산
  * 연산 함수: RDD에 저장되는 데이터를 iterator[T] 형태로 만들어주는 연산 함수를 가짐
* 이 원조 모델에는 몇가지 문제가 있음
  * 연산 함수나 연산식 자체가 투명하지 않음
  * Iterator[T] 데이터 타입이 파이썬 RDD에서 불투명했음
  * 스파크가 함수에서의 연산이나 표현식을 검사하지 못하다 보니 최적화할 방법이 없었음
  * T로 표시한 타입에 대한 정보가 전혀 없었음
* 이러한 방식에 대한 해결이 필요함

## 스파크의 구조 확립
* 스파크 2.x는 구조 확립을 위한 핵심 개념들을 도입함
  * 일상적인 패턴들을 써서 연산을 표현
  * SQL의 테이블이나 스프레드시트처럼 지원하는 정형화 데이터 타입을 써서 표 형태로 구성할 수 있게 되었다는 것.

### 핵심적인 장점과 이득
* 표현성, 단순성, 구성 용이성, 통일성
  ```python
  # 저수준 API 코드를 이해하기가 어렵다.
  # (name, age) 형태의 튜플로 된 RDD를 생성한다.
  dataRDD = sc.parallelize([("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)])
  # 집계와 평균을 위한 람다 표현식과 함께 map과  reduceByKey 트랜스포메이션을 사용한다.
  agesRDD = (dataRDD.map(lambda x: (x[0], (x[1], 1))).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).map(lambda x: (x[0], x[1][0]/x[1][1])))
  
  # 고수준 API
  from pyspark.sql import SparkSession
  from pyspark.sql.functions import avg
  
  spark = (SparkSession.builder.appName("AuthorsAges").getOrCreate())
  
  data_df = spark.createDataFrame([("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)])
  avg_df = data_df.groupBy("name").agg(avg("age"))
  avg_df.show()
  ```

* 상위 수준 API는 컴포넌트들과 언어를 통들어 일관성을 갖는다. 스칼라 코드도 파이썬 코드와 비슷하게 생겼다.
  ```scala
  import org.apche.spark.sql.functions.avg
  import org.apche.spark.sql.SparkSession
  val spark = SparkSession.builder
                          .appName("AuthorsAges")
                          .getOrCreate()
  val dataDF = spark.createDataFrame(Seq(("Brooke", 20),  ("Denny", 31), ("Jules", 30), ("TD", 35))).toDF("name", "age")
  val avgDF = dataDF.groupBy("name").agg(avg("age"))
  avgDF.show()
  ```
  
* 상위 수준 구조화 API 위에 구축된 스파크 SQL 엔진이 있기 때문에 모든 컴포넌트를 이용해도 정형화 데이터 형태인 데이터 프레임으로 변환하고 연산된다.

## 데이터 프레임 API
### 스파크의 기본 데이터 타입
|데이터 타입|파이썬에서 할당되는 값|초기 생성 API|
|--------|---------------|-----------|
|ByteType|int|DataTypes.ByteType|
|ShortType|int|DataTypes.ShortType|
|IntegerType|int|DataTypes.IntegerType|
|LongType|int|DataTypes.LongType|
|FloatType|float|DataTypes.FloatType|
|DoubleType|float|DataTypes.DoubleType|
|StringType|str|DataTypes.StringType|
|BooleanType|bool|DataTypes.BooleanType|
|DecimalType|decimal.Decimal|DecimalType|

### 스파크의 정형화 타입과 복합 타입
|데이터 타입|파이썬에서 할당되는 값|초기 생성 API|
|--------|----------------|----------|
|BinaryType|bytearray|BinaryType()|
|TimestampType|datetime.datetime|TimestampType()|
|DateType|datetime.date|DateType()|
|ArrayType|list, tuple, array 중|ArrayType(datatype, nullable)|
|MapType|dict|MapType(keyType, valueType, nullable|
|StructType|list 혹은 tuple|StructType([fields])|
|StructField|해당 필드와 맞는 값의 타입|StructField(name, dataType, nullable)|

### 스키마와 데이터 프레임 만들기
미리 스키마를 정의하는 것의 장점
* 스파크가 데이터 타입을 추측해야 하는 책임을 덜어줌
* 스키마를 확정하기 위해 별도의 잡을 만드느 것을 방지함
* 데이터가 스키마와 맞지 않는 경우 조기에 문제를 발견함

#### 스키마를 정의하는 두 가지 방법
* 프로그래밍 스타일로 정의
  ```python
  from pyspark.sql.types import *
  schema = StructType([StructField("author", StringType(), False),
                       StructField("title", StringType(), False),
                       StructField("pages", IntegerType(), False)])
  ```
  
* DDL를 사용
  ```python
  schema = "author STRING, title STRING, pages INT"
  ```
  
* 예제
  ```python
  from pyspark.sql import SparkSession
  
  schema = "ID INT, First STRING, Last STRING, Url STRING, Published STRING, Hits INT, Campagins ARRAY<STRING>"
  
  data = [
    [1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "LinkedIn"]],
    ...
  ]
  
  if __name__ == "__main__":
      spark = SparkSession.builder
                          .appName("Example-3_6")
                          .getOrCreate())
      blogs_df = spark.createDataFrame(data, schema)
      blogs_df.show()
      print(blogs_df.printSchema())
  ```
스키마는 직접 입력하지 않고 JSON 파일에서 데이터를 읽어 들여도 동일하다.


### 칼럼과 표현식
* 칼럼을 여러 방면으로 표현할 수 있는데 논리식이나 수학 표현식 `expr("columnName * 5")`을 사용할 수도 있다.
  ```python
  blogs_df.select(expr("Hits") * 2).show(2)
  blogs_df.select(col("Hits") * 2).show(2)
  blogs_df.select(expr("Hits * 2")).show(2)
  blogs_df.withColumn("Big Hitters", (expr("Hits > 10000"))).show()
  print(blogs_df.schema)
  ```
  
### 로우
* Row는 스파크의 객체이고 순서가 있는 필드 집합 객체이다.
  ```python
  from pyspark.sql import Row
  blog_row = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015", ["twitter", "LinkedIn"])
  blog_row[1]
  
  # Row 객체들은 빠른 탐색을 위해 데이터 프레임으로 만들어 사용하기도 함
  rows = [Row("Matei Zaharia", "CA"), Row("Reynold Xin", "CA")]
  authors_df = spark.createDataFrame(rows, ["Authors", "State"])
  authors_df.show()
  ```
  
### 자주 쓰이는 데이터 프레임 작업들
#### DataFrameReader와 DataFrameWriter 사용하기
```python
from pyspark.sql.types import *
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                          StructField('UnitID', StringType(), True),
                          ...
                          StructField('Delay', FloatType(), True)])
# DataFrameReader 인터페이스로 CSV 파일을 읽는다.
sf_fire_file = "~/Downloads/LearningSparkV2-master/chapter3/data/sf-fire-calls.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
```
* DataFrameReader와 DataFrameWriter는 다양한 데이터 소스와 연결해서 사용이 가능함
* DataFrameWriter는 기본 포맷은 Parquet이며 snappy 압축을 사용한다.

#### 데이터 프레임을 파케이 파일이나 SQL 테이블로 저장하기
```python
parquet_path = ...
fire_df.write.format("parquet").save(parquet_path)

parquet_table = ...
fire_df.write.format("parquet").saveAsTable(parquet_table)
```

#### 프로젝션과 필터
* 프로젝션은 필터를 이용해 특정 관계 상태와 매치되는 행들만 되돌려주는 방법
* 프로젝션은 select() 메서드, 필터는 filter()나 where()
```python
few_fire_df = (fire_df.select("IncidentNumber", "AvailableDtTm", "CallType")
                      .where(col("CallType") != "Medical Incideent"))
few_fire_df.show(5, truncate=False)

# 신고 종류가 몇 가지인지 알고 싶다면 countDistinct()를 사용
fire_df.select("CallType")
       .where(col("CallType").isNotNull())
       .agg(countDistinct("CallType").alias("DistinctCallTypes"))
       .show()

# null이 아닌 개별 CallType 추출
fire_df.select("CallType")
       .where(col("CallType").isNotNull())
       .distinct()
       .show(10, False)
```

#### 칼럼 이름 변경 및 추가 삭제
* 파케이 파일 포맷으로 데이터를 작성할 때 칼럼 이름의 공백은 안됨
```python
# StructField를 써서 스키마 내에서 변경
# withColumnRenamed() 함수를 써서 변경
# Delay 칼럼을 ResponseDelayedinMins로 변경 예제
new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
new_fire_df.select("ResponseDelayedinMins")
           .where(col("ResponseDelayedinMins") > 5)
           .show(5,False)

# 데이터 타입 변경
fire_ts_df = new_fire_df.withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
                        .drop("CallDate")
                        .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
                        .drop("WatchDate")
                        .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))

# 변경된 데이터 타입에 따라 원하는 년도 가져오기
fire_ts_df.select(year("IncidentDate"))
          .distinct()
          .orderBy(year("IncidentDate"))
          .show()
```

#### 집계연산
```python
# 가장 흔한 타입의 신고
fire_ts_df.select("CallType")
          .where(col("CallType").isNotNull())
          .groupBy("CallType")
          .count()
          .orderBy("count", ascending=False)
          .show(n=10, truncate=False)
```

#### 그외 데이터 프레임 연산들
* min(), max(), sum(), avg() 등 일반적인 연산들
* stat(), describe(), correlation(), covariance(), sampleBy(), approxQuantile(), frequentItems() 등도 있음


## 데이터세트 API
* 데이터세트는 정적 타입 API와 동적 타입 API 특성을 모두 가짐

### 정적타입 객체, 동적 타입 객체, 포괄적인 Row
* 데이터 세트는 자바와 스칼라에서 사용
* 파이썬과 R에서는 데이터 프레임만 사용 가능
* Row는 포괄적 객체 타입이며 인덱스를 사용하여 접근
```python
from pyspark.sql import Row
row = Row(350, True, "Learning Spark 2E", None)

print(row[0])
print(row[1])
print(row[2])
```

### 데이터 프레임 vs 데이터 세트
* 컴파일 타임에 엄격한 타입 체크를 원하며 특정 Dataset[T]를 위해 여러 개의 케이스 클래스를 만드는 것에 부담이 없다면 데이터 세트를 사용
* 자신의 작업이 SQL과 유사한 질의를 쓰는 관계형 변환을 필요한다면 데이터 프레임 사용
* 프로젝트 텅스텐의 직렬화 능력을 통한 이득을 보고 싶다면 데이터 세트를 사용
* 코드 최적화, API 단순화를 원하면 데이터 프레임 사용
* 파이썬 사용자는 데이터 프레임을 쓰되 제어권을 갖고 싶다면 RDD로
* 정형화 API 사용 시 오류가 발견되는 시점

||SQL|데이터 프레임|데이터 세트|
|-|---|---------|--------|
|문법 오류|실행 시점|컴파일 시점|컴파일 시점|
|분석 오류|실행 시점|실행 시점|컴파일 시점|

### 언제 RDD를 사용하는가
* RDD를 사용하돌록 작성된 서드파티 패키지 사용시
* 코드 최적화, 단순화 등의 이득을 포기할 수 있을 때
* 스파크가 어떻게 질의를 수행할지 지정해 주고 싶을 때 

## 스파크 SQL과 하부의 엔진
* 스파크 SQL은 정형화 데이터에 ANSI SQL 2003을 호환함
* 하이브 메타스토어와 테이블에 접근
* 정형화된 파일 포맷(JSON, CSV, 텍스트, Avro, Parquet, ORC 등)의 데이터를 읽고 쓰 수 있음
* JDBC/ODBC 커넥터를 통해 외부의 도구들과 연결
* 질의 계획과 JVM을 위한 최적화된 코드 생성

### 카탈리스트 옵티마이저
* 연산 쿼리를 받아 실행 계획으로 변환함
  * 분석
    * SQL이나 데이터 프레임 쿼리를 위한 추상 문법 트리 (Abstract Syntax Tree, AST) 생성
  * 논리적 최적화
    * 내부적으로 두 가지 단계로 이루어짐
      * 여러 계획들을 수립하고 비용 기반 옵티마이저를 써서 비용을 책정
      * 연산 트리들로 배열되고 이 계획들은 물리 계획 수립의 입력 데이터가 됨
  * 물리 계획 수립
    * 스파크 실행 엔진에서 선택된 논리 계획을 바탕으로 대응되는 최적화된 물리 계획 생성
  * 코드 생성
    * 효율적인 자바 바이트 코드를 생성
    * 메모리에 올라와있는 데이터를 다루므로 실행 속도를 높이기 위한 컴파일러 기술을 사용. 이는 컴파일러처럼 동작하고 포괄 코드 생성을 가능하게 하는 텅스텐이 역할을 함
    * 텅스텐은 물리적 쿼리 최적화 단계로  전체 쿼리를 하나의 함수로 합치면서 CPU 레지스터 사용을 없애고 최종 실행 시 콤팩트한 RDD 코드를 생성한다.