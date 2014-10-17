import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._ 
import java.text.SimpleDateFormat

object SparkJoin{
  def main(args: Array[String]) {
    val s = System.nanoTime
    val conf = new SparkConf().setAppName("SparkJoin")
    val sc = new SparkContext(conf)
    def parseDate(value:String) = {
      try{
      	Some(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(value))
      } catch {
      	case e: Exception => None
      } 
    }

    def trimString(value:String) = {
      try{
        Some(value.substring(1,value.length()-1))
      } catch {
      	case e: Exception => None
      }
    }

    //val input1 = "hdfs:///attribution_shared/campaigns/1_1/0/0/0/prod.campaigns.1_1.0.0.0.csv.gz" //option for importing file from hdfs
    val input1 = "s3n://mat-log-csv-prod/olap_log/join_table/prod/attribution_shared/campaigns/1_1/0/0/0/prod.campaigns.1_1.0.0.0.csv.gz"
      
    val join1 = sc.textFile(input1)
    val join1key = join1.map(line => {
          val fields = line.split("\t")
          fields
        }).filter( f => !trimString(f(0)).isEmpty && f.length == 47 ).map(f => (trimString(f(0)).get.toInt, f(5))).keyBy( tup => tup._1)

    //val input2 = "hdfs:///attribution_shared/publishers/1_1/0/0/0/prod.publishers.1_1.0.0.0.csv.gz"
    val input2 = "s3n://mat-log-csv-prod/olap_log/join_table/prod/attribution_shared/publishers/1_1/0/0/0/prod.publishers.1_1.0.0.0.csv.gz"
    val join2 = sc.textFile(input2)
    val join2key = join2.map(line => {
          val fields = line.split("\t")
          (fields(0).substring(1,fields(0).length()-1).toInt, fields(6))
        }).keyBy( tup => tup._1).leftOuterJoin(join1key)

    join2key.saveAsTextFile("/mnt/result")

    //merge results and copy to local disk
    //hadoop fs -getmerge hdfs://ec2-54-166-204-210.compute-1.amazonaws.com:9000/mnt/result /mnt/result

    println("time: "+(System.nanoTime-s)/1e6+"ms")
    sc.stop()
  }
}


