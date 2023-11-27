# Chapter 1 - Introduction to Spark

## Contents

### Start of spark

#### 1. Google, google, google

구글이 GFS, 맵리듀스(MR), 빅테이블 등을 만들어 내어 논문 소개  
특히, 맵리듀스는 함수형 프로그래밍 개념을 기반으로 GFS와 빅테이블 위에서 대규모 데이터 분산 처리 병렬 프로그래밍 패러다임

맵리듀스 어플리케이션은 data를 pulling하여 처리하지 않고, logic 또는 source code를 push하여 데이터가 있는 곳에서 처리되게 함

클러스터 워커노드들은 중간 연산을 통해 집계, 결과를 합쳐 리듀스 함수에서 최종결과 생산해 이를 분산 저장소에 기록 -> 네트워크 트래픽 감소, 로컬 디스크에 대한 IO 극대화

#### 2. Hadoop in Yahoo!

1의 소개된 개념들은 하둡 프레임워크의 출현으로 이어짐 (하둡 공통모듈, 맵리듀스, HDFS, 아파치 하둡 YARN)

그러나, 하둡은 아래와 같은 단점들이 있었음

- 높아진 운영 복잡도로 관리의 어려움
- 엄청나게 길어진 코드(셋업코드) 불안정안 장애대응
- 빈번한 디스크 I/O(중간단계 저장)로 엄청나게 길어진 작업시간
- 배치처리로는 적당하나, 머신러닝, 스트리밍, SQL 질의등의 워크로드 적용의 한계

이때문에 다양한 워크로드에 맞는 시스템들이 탄생하게 됨 (Hive, Impala, Giraph, Drill, Mahout 등)

#### 3. Emergence of spark

Speed

- DAG
- Tungsten
- RAM

Easy to Use

- RDD

Modularity

- One Engine

Scalability

- Third-party connectors

Java, R, Scala, Python, SQL -> lightweighted bytecode.

SparkSQL(DataFrame&DataSet)  
SparkStreaming  
SparkML  
GraphX

#### 4. Distributed Execution of spark

Spark Driver

SparkSession

Cluster Manager

Spark Executor

##### Deploy mode
