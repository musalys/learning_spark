# 스파크 SQL과 데이터세트

## 자바와 스칼라를 위한 단일 API

- 데이터세트는 강력한 형식의 객체를 위하여 통합되고 단일화된 API를 제공
- 스칼라 & 자바만이 강력한 타입을 지원
- 파이썬 & R은 형식화되지 않은 타입의 데이터 프레임 API를 지원하고 있다.
- 데이터세트는 DSL 연산자나 함수형 프로그래밍을 사용하여 병렬로 작동할 수 있는 도메인별 형식화된 객체
## 데이터세트를 위한 스칼라 케이스 클래스와 자바빈

- 스파크는 String, Binary, Integer, Boolean, Map 같은 내부적 타입을 가지고 있음
- 위는 스파크 작업 중에 스칼라 및 자바의 언어별 데이터 타입에 원활하게 매핑하는 데 사용됨
- Dataset[T]를 생성하기 위해, 객체를 정의하는 case class가 필요
- 행에 대한 모든 개별 칼럼 이름과 유형을 알아야 하기 때문에 사전 고려 필요
- 데이터세트 API에서는 미리 데이터 유형을 정희하고, 케이스 클래스 또는 자바빈 클래스가 스키마와 일치해야함
- 스칼라 케이스 클래스 또는 자바 클래스 정의의 필드 이름은 데이터 원본의 순서와 일치해야함.

```scala
case class Blogger(
	id: BigInt,
	first: String,
	last: String,
	url: String,
	published: String,
	hits: BigInt,
	campaigns: Array[String]
)
// In Databricks Community Edition DBFS
val bloggers = "/FileStore/tables/blogs.json"
val bloggersDS = spark.read.format("json")
    .option("path", bloggers)
    .load()
    .as[Blogger]
```

## 데이터세트 작업

### 샘플 데이터 생성

- 샘플 데이터세트를 생성하는 간단한 방법 SparkSession 인스턴스를 사용하는 것
- 자바에서는 명시적 _인코더_ 를 사용해야 한다(스칼라 스파크에서는 암시적으로 처리함)

```scala
import scala.util.Random._

case class Usage(uid: Int, uname: String, usage: Int)

val r = new scala.util.Random(42)

val data = for (i <- 0 to 1000)
  yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""), r.nextInt(1000)))

val dsUsage = spark.createDataset(data)
dsUsage.show(10)
```
### 샘플 데이터 변환

- 데이터세트는 객체의 정형화된 컬렉션이므로, 함수적인 또는 관계적인 연산을 사용하여 병렬로 변환 가능
	- 이러한 변환의 예로는 map(), reduce(),filter(),select(),aggregate()가 있음
- 스칼라는 함수형 프로그래밍 언어이며, 최근에 람다, 함수형 인수, 클로저가 자바에도 추가됨
#### 고차 함수 및 함수형 프로그래밍

고차 함수 및 데이터세트 사용에 대해 몇가지 주의점
- 입력된 JVM 객체를 함수에 대한 인수로 사용하고 있다.
- 읽기 쉽게 만드는 (객체 지향 프로그래밍에서) 도트 표기법 사용하여 형식화된 JVM 객체 내의 개별 필드에 액세스
- 일부 기능 및 람다 시그니처는 type-safe이 보장되어 컴파일 시점 오류 감지를 보장
- 자바나 스칼라의 고차 함수 생성자가 없이도 map(), filter()와 동등한 기능을 제공하므로 데이터세트 또는 데이터프레임에서 함수형 프로그래밍을 사용할 필요가 없음. 대신 조건부 DSL 연산자나 SQL 표현식을 사용할 수 있다
- 데이터세트의 경우 데이터 유형에 대한 JVM과 스파크의 내부 이진 형식 간에 데이터를 효율적으로 변환하는 메커니즘인 인코더를 사용
#### 데이터프레임을 데이터세트로 변환

쿼리 및 구조의 강력한 유형 확인을 위해 데이터프레임을 데이터세트로 변환할 수 있다. df.as[SomeCaseClass] 표기법을 사용하면 된다.

## 데이터세트 및 데이터프레임을 위한 메모리 관리

- 스파크는 집중적인 인메모리 분산 빅데이터 엔진으로, 메모리를 효율적으로 사용하는 것은 실행 속도에 큰 영향을 줌
- spark 1.0은 메모리 스토리지, 직렬화 및 역직렬화에 RDD 기반 자바 객체를 사용하였음 -> 이는 리소스 측면에서 비용이 많이 들고 속도가 느렸다. 또한, 스토리지가 **자바 힙**에 할당되었기 때문에 대규모 데이터세트에 대한 JVM의 가비지 컬렉션에 좌우되었다.
- 스파크 1.x에 도입된 프로젝트 텅스텐의 두드러진 특징중 하나는 오프셋과 포인터를 사용하여 오프 힙 메모리에 데이터세트와 데이터 프레임을 배치하는 새로운 내부 행 기반 형식이었다. 스파크는 **인코더**라고 불리는 효율적인 메커니즘을 사용하여 JVM과 내부 텅스텐 포맷 사이를 직렬화하고 역직렬화한다. 오프 힙에 메모리를 직접 할당한다는 것은 스파크가 GC에 의해 받는 옇양을 줄일 수 있다는 것을 의미한다.
- 스파크 2.x는 전체 단계 코드 생성 및 벡터화된 컬럼 기반 메모리 레이아웃을 특징으로 하는 2세대 텅스텐 엔진을 도입했다. 최신 컴파일러의 아이디어와 기술을 기반으로 제작된 이 새로운 버전은 빠른 병렬 데이터 액세스를 위하여 '단일 명령, 다중 데이터(SIMD)' 접근 방식의 최신 CPU 및 캐시 아키텍쳐를 활용했다.
## 데이터 집합 인코더

인코더는 오프 힘 메모리의 데이터를 스파크의 내부 텅스텐 포맷에서 JVM 자바 오브젝트로 변환한다. 즉, 스파크의 내부 형식에서 원시 데이터 유형을 포함한 JVM 객체로 데이터세트 객체를 직렬화하고 역직렬화한다. 예를 들어, Encoder[T]는 스파크의 내부 텅스텐 형식에서 Dataset[T]로 변환된다.

스파크는 원시 유형, 스칼라 케이스 클래스 및 자바빈에 대한 인코더를 자동으로 생성할 수 있는 내장 지원 기능을 가지고 있다. 자바와 크리오 직렬화, 역직렬화에 비교했을 때 스파크 인코더는 상당히 빠르다.

스칼라의 경우에는 스파크가 이러한 효율적인 변환을 위해 바이트 코드를 자동으로 생성한다.

### 스파크의 내부 형식과 자바 객체 형식 비교

자바 객체에는 헤더 정보, 해시코드, 유니코드 정보 등 큰 오버헤드가 있다. 'abcd'와 같은 간단한 자바 문자열도 예상하는 4바이트 대신 48바이트의 스토리지를 사용한다.

스파크는 데이터세트 또는 데이터프레임을 위한 JVM 기반 객체를 생성하는 대신 **오프 힙** 자바 메모리를 할당하여 데이터를 레이아웃하고, 인코더를 사용하여 데이터를 메모리 내 표현해서 JVM 객체로 변환한다.

데이터가 이러한 인접한 방식으로 저장되고 포인터 산술과 오프셋을 통해 액세스할 수 있을 때, 인코더는 데이터를 빠르게 직렬화하거나 역직렬화할 수 있다.

### 직렬화 및 역직렬화

송신자: 이진표시 또는 형식으로 인코딩(직렬화)
수신자: 바이너리 형식에서 해당 데이터 형식 객체로 디코딩(역직렬화)

- 스파크 내부 텅스텐 이진 형식(그림 6-1 및 6-2 참고)은 자바 힙 메모리에서 객체를 저장하며, 크기가 작아 공간을 적게 차지
- 인코더는 메모리 주소와 오프셋이 있는 간단한 포인터 계산을 사용해 메모리를 가로질러 빠르게 직렬화할 수 있음
- 수신 단부에서 인코더는 스파크의 내부 표현으로 이진 표현을 빠르게 역직렬화할 수 있기 때문에 JVM의 가비지 컬렉션의 일시 중지로 인한 방해를 받지 않음

## 데이터세트 사용 비용

데이터세트가 람다 또는 함수형 인수를 사용하는 filter(), map() 또는 flatMap()과 같은 고차 함수에 전달될 때 스파크 내부의 텅스텐 형식에서 JVM 객체로 역직렬화하는 비용이 발생. 이 비용은 경미하고 감수할만 하나, 대규모 데이터세트와 많은 쿼리에 걸쳐 이러한 비용이 발생하면 성능에 영향을 미칠 수 있음

### 비용 절감 전략

1. 쿼리에서 DSL 표현을 사용하고 람다를 고차 함수에 대한 인수로 과도하게 사용하여 익명성을 높이는 것을 피해야함 (카탈리스트 옵티마이저가 쿼리를 최적화할 수 없다)
2. 직렬화 및 역직렬화가 최소화되도록 쿼리를 함께 연결하는 것

그림  6-3에서 볼 수 있듯이, 람다에서 DSL로 이동할 때 마다 Person JVM 객체를 직렬화하고 역직렬화하는 비용이 발생된다.