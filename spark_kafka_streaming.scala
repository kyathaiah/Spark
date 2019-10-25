
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object kafkaConsumer {

def main(args:Array[String] ){

	val spark = SparkSession.builder.appName(getClass.getSimpleName).getOrCreate()
	val sc = spark.sparkContext
	val ssc = new StreamingContext(sc, Seconds(30))	
	
	case class attacker(ip: String, msg: String)
	val sqlContext = new org.apache.spark.sql.SQLContext(sc)
	
	val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181","spark-streaming-group", Map("test" -> 1))
	val words=kafkaStream.map(_._2)	
			
	words.foreachRDD(rdd => {
    var l = rdd.map(_.split(" "))
    val pdf = l.map(p => attacker(p(0),p(1)))
    val attackerdf = sqlContext.createDataFrame(pdf)
    val ips = attackerdf.groupBy("ip").count.filter($"count">10)	
	ips.show() 
    ips.write.mode("overwrite").csv("D:\\k\\attaker_output")	
    })	
	
	ssc.start()
	ssc.awaitTermination()
}

}
