# 2장 아파치 스파크 다운로드 및 시작

* 로컬 모드를 이용

## 1단계: 아파치 스파크 다운로드
* https://spark.apache.org/downloads.html 
* PySpark
  ```shell
  pip install pyspark
  pip install pyspark[sql,ml,mllib]
  ```
* 스파크 디렉터리와 파일들
  * README.md : 스파크 셸을 어떻게 사용하는지, 어떻게 빌드하는지, 단독 스파크를 실행하는지 등 여러 설명들을 담고 있음
  * bin : 스파크 셸들을 포함(spark-sql, pyspark, spark-shell, sparkR) 스파크와 상호 작용할 수 있는 대부분의 스크립트를 갖고 있음. spark-submit을 써서 스파크 애플리케이션을 제출하거나 쿠버네티스로 실행할 때 도커 이미지를 만들고 푸시하는 스크립트 작성을 위해 사용
  * sbin : 대부분의 스크립트는 다양한 배포 모드에서 클러스터의 스파크 컴포넌트들을 시작하고 중지하기 위한 관리 목적
  * kubernetes : 쿠버네티스 클러스터에서 쓰는 도커 이미지 제작을 위한 Dockerfile들을 담고 있음
  * data : MLlib, 정형화 프로그래밍, GraphX 등에서 입력으로 사용되는 *.txt 파일이 있음
  * examples : 입문하기 좋은 예제 코드들과 문서들

## 2단계: 스파크 혹은 파이스파크 셸 사용
* pyspark, spark-shell, spark-sql, sparkR 인터프리터들로 일회성 데이터 분석이 가능함
* 클러스터에 연결하고 분산 데이터를 워커 노드의 메모리에 로드할 수 있도록 확장되어옴.
* PySpark를 시작하려면 bin 디렉터리로 가서 pyspark를 실행
  ```shell
  # PySpark
  $ ./pyspark
  
  # Scala
  $ ./spark-shell
  ```
  
## 3단계: 스파크 애플리케이션 개념 이해
* 애플리케이션 : API를 써서 스파크 위에서 돌아가는 사용자 프로그램. 드라이버 프로그램과 클러스터의 실행기로 이루어짐
* SparkSession : 스파크 코어 기능들과 상호 작용할 수 있는 진입점을 제공하며 API로 프로그래밍을 할 수 있게 해주는 객체. 스파크 드라이버는 기본적으로 SparkSession을 제공하지만 애플리케이션에서는 사용자가 SparkSession 객체를 생성해서 써야함
* 잡 (Job) : 스파크 액션에 대한 응답으로 생성되는 여러 태스크로 이루어진 병렬 연산
* 스테이지 : 각 잡은 스테이지라 불리는 서로 의존성을 가지는 다수의 태스크 모음으로 나뉨
* 태스크 : 스파크 이그제큐터로 보내지는 작업 실행의 가장 기본적인 단위
* 스파크 애플리케이션과 SparkSession
  * 모든 스파크 애플리케이션의 핵심에는 스파크 드라이버 프로그램이 있으며, 이 드라이버는 SparkSession 객체를 만든다
  * SparkSession 객체를 만들었으면 그를 통해 스파크 연산을 수행하는 API를 써서 프로그래밍이 가능하다.
* 스파크 잡
  * 스파크 셸로 상호 작용하는 작업 동안 드라이버는 스파크 애플리케이션을 하나 이상의 스파크 잡으로 변환함.
  * 각 잡은 DAG로 변환됨
* 스파크 스테이지
  * 어떤 작업이 연속적으로 또는 병렬적으로 수행되는지에 맞춰 스테이지에 해당하는 DAG 노드가 생성된다.
  * 종종 스파크 이그제큐터끼리의 데이터 전송이 이루어지는 연산 범위 경계 위에서 스테이지가 결정되기도 함.
* 스파크 태스크
  * 각 스테이지는 최소 실행 단위며 스파크 이그제큐터들 위에서 연합 실행되는 스파크 태스크들로 구성됨.
  * 각 태스크는 개별 CPU 코어에 할당되고 데이터의 개별 파티션을 갖고 작업한다.

## 트랜스포메이션, 액션, 지연 평가
* 분산 데이터의 스파크 연산은 트랜스포메이션과 액션으로 구분됨.
* 트랜스포메이션은 원본 데이터를 수정하지 않고 하나의 스파크 데이터 프레임을 새로운 데이터 프레임으로 변형한다. select() 나 filter() 같은 연산은 원본 데이터 프레임을 수정하지 않고 새로운 데이터 프레임으로 결과를 되돌려줌.
* 모든 트랜스포메이션은 뒤늦게 평가된다. 그 결과는 즉시 계산되는게 아니라 계보(lineage)라 불리는 형태로 기록된다. 기록된 리니지는 실행 계획에서 스파크가 더 효율적으로 실행할 수 있도록 최적화한다.
* 지연 평가는 액션이 실행되는 시점이나 데이터에 실제 접근하는 시점까지 실제 실행을 미루는 스파크의 전략임
* 하나의 액션은 모든 기록된 트랜스포메이션의 지연 연산을 발동시킴
* 지연 평가는 스파크가 사용자의 연계된 트랜스포메이션들을 살펴봄으로써 쿼리 최적화를 가능하게 하는 반면, 리니지와 데이터 불변성은 장애에 대한 데이터 내구성을 제공함
* 트랜스포메이션과 액션에 해당하는 일부 예시  
* 
  |트랜스포메이션|액션|
  |---------|---|
  |orderBy()|show()|
  |groupBy()|take()|
  |filter()|count()|
  |select()|collect()|
  |join()|save()|
* 액션과 트랜스포메이션들은 쿼리 계획이 만들어지는 데에 도움을 준다. 하나의 쿼리 계획 안의 어떤 것도 액션이 호출되기 전에는 실행되지 않는다
* 예제에서는 filtered.count() 가 입력되기 전까지 아무것도 셸에서 실행되지 않는다.
  ```python
  strings = spark.read.text("../README.md")
  filtered = strings.filter(strings.value.contains("Spark"))
  filtered.count()
  20
  ```
* 좁은/넓은 트랜스포메이션
  * 트랜스포메이션은 스파크가 지연 평가하는 연산 종류이다. 스파크가 연산 쿼리를 분석하고 어디를 최적화할지 알 수 있다는 이점이 있다.
  * 트랜스포메이션은 좁은 의존성과 넓은 의존성으로 분류할 수 있다.
  * 하나의 입력 파티션을 연산하여 하나의 결과 파티션을 내놓는다면 어느 것이든 좁은 트랜스포메이션이다. filter()와 contains()는 하나의 파티션을 처리하여 결과를 생성하므로 좁은 트랜스포메이션이라고 할 수 있다.
  * groupBy()나 orderBy()는 넓은 트랜스포메이션이다.

## 스파크 UI
* 스파크는 GUI를 써서 애플리케이션을 살펴볼 수 있게 해주며 다양한 레벨에서 확인 가능하다.
* 기본적으로 4040 포트를 사용하는데 다음과 같은 내용을 볼 수 있다
  * 스케줄러의 스테이지와 태스크 목록
  * RDD 크기와 메모리 사용의 요약
  * 환경 정보
  * 실행 중인 이그제큐터 정보
  * 모든 스파크 SQL 쿼리

## 예제
* M&M 개수 집계
  ```python
  import sys
  from pyspark.sql import SparkSession

  if __name__ == "__main__":
      if len(sys.argv) != 2:
          print("Usage: mnmcount <file>", file=sys.stderr)
          sys.exit(-1)

      spark = (SparkSession.builder
                           .appName("PythonMnMCount")
						   .getOrCreate())
  
	  mnm_file = sys.argv[1]
	  mnm_df = (spark.read.format("csv")
						  .option("header", "true")
						  .option("inferSchema", "true")
						  .load(mnm_file))
      count_mnm_df = (mnm_df.select("State", "Color", "Count")
							.groupBy("State", "Color")
							.sum("Count")
							.orderBy("sum(Count)", ascending=False))
      count_mnm_df.show(n=60, truncate=False)
      print("Total Rows = %d" % (count_mnm_df.count()))

      ca_count_mnm_df = (mnm_df.select("State", "Color", "Count")
							   .where(mnm_df.State == "CA")
							   .groupBy("State", "Color")
							   .sum("Count")  
							   .orderBy("sum(Count)", ascending=False))
      ca_count_mnm_df.show(n=10, truncate=False)
      spark.stop()
  ```