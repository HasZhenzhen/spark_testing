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

    def leftJoin(value:scala.collection.Map[Int,(Int,String)],key:String): String = {
      if(key != "\\N"){
        if(value.contains(key.toInt))
    		  value.get(key.toInt).get._2
        else
    		  "\\N"
      }
      else 
        "\\N"
    }

    // Load small tables into RDD in memory
    val input1 = "s3n://mat-log-csv-prod/olap_log/join_table/prod/attribution_shared/campaigns/1_1/0/0/0/prod.campaigns.1_1.0.0.0.csv.gz" 
    val join1 = sc.textFile(input1)
    val join1key = join1.map(line => {
          val fields = line.split("\t")
          fields
        }).filter( f => !trimString(f(0)).isEmpty && f.length == 47 ).map(f => (trimString(f(0)).get.toInt, f(5))).keyBy( tup => tup._1).persist()
    val join1map = sc.broadcast(join1key.collectAsMap)

    val input2 = "s3n://mat-log-csv-prod/olap_log/join_table/prod/attribution_shared/publishers/1_1/0/0/0/prod.publishers.1_1.0.0.0.csv.gz"
    val join2 = sc.textFile(input2)
    val join2key = join2.map(line => {
          val fields = line.split("\t")
          (fields(0).substring(1,fields(0).length()-1).toInt, fields(6))
        }).keyBy( tup => tup._1).persist()
    val join2map = sc.broadcast(join2key.collectAsMap)

    val input3 = "s3n://mat-log-csv-prod/olap_log/join_table/prod/attribution_shared/advertiser_sub_publishers/1_1/0/0/0/prod.advertiser_sub_publishers.1_1.0.0.0.csv.gz"
    val join3 = sc.textFile(input3)
    val join3key = join3.map(line => {
          val fields = line.split("\t")
          (fields(0).substring(1,fields(0).length()-1).toInt, fields(2))
        }).keyBy( tup => tup._1).persist()
    val join3map = sc.broadcast(join3key.collectAsMap)

    val input4 = "s3n://mat-log-csv-prod/olap_log/join_table/prod/attribution_shared/advertiser_sub_sites/1_1/0/0/0/prod.advertiser_sub_sites.1_1.0.0.0.csv.gz"
    val join4 = sc.textFile(input4)
    val join4key = join4.map(line => {
          val fields = line.split("\t")
          (fields(0).substring(1,fields(0).length()-1).toInt, fields(2))
        }).keyBy( tup => tup._1).persist()
    val join4map = sc.broadcast(join4key.collectAsMap)

    val input5 = "s3n://mat-log-csv-prod/olap_log/join_table/prod/attribution_shared/advertiser_sub_campaigns/1_1/0/0/0/prod.advertiser_sub_campaigns.1_1.0.0.0.csv.gz"
    val join5 = sc.textFile(input5)
    val join5key = join5.map(line => {
          val fields = line.split("\t")
          fields
        }).filter(f => f.length == 8).map(f => (f(0).substring(1,f(0).length()-1).toInt, f(2))).keyBy( tup => tup._1).persist()
    val join5map = sc.broadcast(join5key.collectAsMap)

    val input6 = "s3n://mat-log-csv-prod/olap_log/join_table/prod/attribution_shared/countries/1_1/0/0/0/prod.countries.1_1.0.0.0.csv.gz"
    val join6 = sc.textFile(input6)
    val join6key = join6.map(line => {
          val fields = line.split("\t")
          (fields(0).substring(1,fields(0).length()-1).toInt, fields(2))
        }).keyBy( tup => tup._1).persist()
    val join6map = sc.broadcast(join6key.collectAsMap)


    // Create the context with 1 file from the directory
    val lines = sc.textFile("s3n://mat-log-csv-prod/olap_log/log/prod/s3_tracking_log_csv/stat_clicks/1_12/883/2014/06/*.gz")

    val newtable = lines.map(line => {
          val fields = line.split("\t")
          fields
        }).filter(f=>f.length==155).filter( f => ( !parseDate(f(123).substring(1,f(123).length()-1)).get.before(parseDate("2014-06-01 00:00:00").get) && parseDate(f(123).substring(1,f(123).length()-1)).get.before(parseDate("2014-07-01 00:00:00").get)) ).map( f => (f(20), f(15), f(104), f(68), f(70), f(120), f(109), f(110), f(35), f(36), f(37), f(105), f(19)) ).map( f => List(leftJoin(join1map.value,f._1), leftJoin(join2map.value, f._2), f._3, f._4, f._5, f._6, f._7, f._8, leftJoin(join3map.value,f._9), leftJoin(join4map.value,f._10), leftJoin(join5map.value,f._11), leftJoin(join6map.value,f._12), f._13, f._1, f._2, f._9, f._10, f._11) ).map(f => f.mkString("","\t",""))

    newtable.saveAsTextFile("/mnt/result")

    println("time: "+(System.nanoTime-s)/1e6+"ms")
    sc.stop()
  }
}
