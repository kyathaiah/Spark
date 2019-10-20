import spark.implicits._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Encoders
import org.apache.spark.streaming._
val ssc = new StreamingContext(sc, Seconds(30))
val lines = ssc.textFileStream("/dev/solenis/sol_d_test/str_test/")

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

def proc() = {

val aalog = sqlContext.read.format("csv").option("header", "false").option("delimiter", " ").option("inferSchema","true").load("/dev/solenis/sol_d_test/str_test/output.txt")	

println ("Printing dataframe")

val attacker = aalog.groupBy("_c0").count.filter($"count">2)

attacker.show()
attacker.write.mode("overwrite").csv("/dev/solenis/sol_d_test/attacker_ip")

}

lines.foreachRDD { rdd => proc()}
ssc.start()