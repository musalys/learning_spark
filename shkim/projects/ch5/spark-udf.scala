import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder
  .appName("UDFTest")
  .getOrCreate()

// 큐브 함수 생성
val cubed = (s: Long) => s * s * s

// UDF로 등록
spark.udf.register("cubed", cubed)

// 임시 뷰 생성
spark.range(1, 9).createOrReplaceTempView("udf_test")

// 큐브 UDF를 사용하여 쿼리
spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()