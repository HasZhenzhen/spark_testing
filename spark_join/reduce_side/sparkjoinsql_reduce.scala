//after compilation, run with /root/spark/bin/spark-submit --class "SparkJoin" --master spark://ec2-54-234-24-96.compute-1.amazonaws.com:7077 /root/zhenzhen/sparkjoinsql_reduce/target/scala-2.10/spark-join_2.10-1.1.jar 
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import java.text.SimpleDateFormat
import scala.util.control.Exception.allCatch


object SparkJoin{
	// Define the schema using a case class.
	// Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit, 
	// you can use custom classes that implement the Product interface.
	case class campaigns(ID: Int, NAME: String)
	case class publishers(ID: Int, NAME: String)
	case class publisher_sub_publishers(ID: Int, NAME: String)
	case class publisher_sub_sites(ID: Int, NAME: String)
	case class publisher_sub_campaigns(ID: Int, NAME: String)
	case class countries(ID: Int, NAME: String)
	case class Clicks(CAMPAIGN_ID: String, PUBLISHER_ID: String, IS_UNIQUE: String, IOS_IFA: String, IOS_IFA_SHA1: String, USER_AGENT: String, WURFL_DEVICE_OS: String, WURFL_DEVICE_OS_VERSION: String, PUBLISHER_SUB_PUBLISHER_ID: String, PUBLISHER_SUB_SITE_ID: String, PUBLISHER_SUB_CAMPAIGN_ID: String, COUNTRY_ID: String, SITE_ID: String)

	def main(args: Array[String]) {
		/*if (args.length < 1) {
      		System.err.println("Usage: SparkJoin <output>")
      		System.exit(1)
    	}*/
    	val s = System.nanoTime
		val conf = new SparkConf().setAppName("SparkJoin")
		val sc = new SparkContext(conf)

		// useful Functions
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
		def isInt(s: String): Boolean = (allCatch opt s.toInt).isDefined
		def time[A](f: => A) = {
		  val s = System.nanoTime
		  val ret = f
		  println("time: "+(System.nanoTime-s)/1e6+"ms")
		  ret
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

		//read in csv file
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		import sqlContext._

		val input1 = "s3n://mat-log-csv-prod/olap_log/join_table/prod/attribution_shared/campaigns/1_1/0/0/0/prod.campaigns.1_1.0.0.0.csv.gz"
		val join1 = sc.textFile(input1)
		val join1key = join1.map(line => {
			val fields = line.split("\t")
			fields
			}).filter( f => !trimString(f(0)).isEmpty && f.length == 47 ).map(f => (trimString(f(0)).get.toInt, f(5))).map(f => new campaigns(f._1, f._2))

		val input2 = "s3n://mat-log-csv-prod/olap_log/join_table/prod/attribution_shared/publishers/1_1/0/0/0/prod.publishers.1_1.0.0.0.csv.gz"
		val join2 = sc.textFile(input2)
		val join2key = join2.map(line => {
			val fields = line.split("\t")
			(fields(0).substring(1,fields(0).length()-1).toInt, fields(6))
			}).map(f => new publishers(f._1, f._2))

		val input3 = "s3n://mat-log-csv-prod/olap_log/join_table/prod/attribution_shared/advertiser_sub_publishers/1_1/0/0/0/prod.advertiser_sub_publishers.1_1.0.0.0.csv.gz"
		val join3 = sc.textFile(input3)
		val join3key = join3.map(line => {
			val fields = line.split("\t")
			(fields(0).substring(1,fields(0).length()-1).toInt, fields(2))
			}).map(f => new publisher_sub_publishers(f._1, f._2))

		val input4 = "s3n://mat-log-csv-prod/olap_log/join_table/prod/attribution_shared/advertiser_sub_sites/1_1/0/0/0/prod.advertiser_sub_sites.1_1.0.0.0.csv.gz"
		val join4 = sc.textFile(input4)
		val join4key = join4.map(line => {
			val fields = line.split("\t")
			(fields(0).substring(1,fields(0).length()-1).toInt, fields(2))
			}).map(f => new publisher_sub_sites(f._1, f._2))

		val input5 = "s3n://mat-log-csv-prod/olap_log/join_table/prod/attribution_shared/advertiser_sub_campaigns/1_1/0/0/0/prod.advertiser_sub_campaigns.1_1.0.0.0.csv.gz"
		val join5 = sc.textFile(input5)
		val join5key = join5.map(line => {
			val fields = line.split("\t")
			fields}).filter(f => f.length == 8).map(f => (f(0).substring(1,f(0).length()-1).toInt, f(2))).map(f => new publisher_sub_campaigns(f._1, f._2))

		val input6 = "s3n://mat-log-csv-prod/olap_log/join_table/prod/attribution_shared/countries/1_1/0/0/0/prod.countries.1_1.0.0.0.csv.gz"
		val join6 = sc.textFile(input6)
		val join6key = join6.map(line => {
			val fields = line.split("\t")
			(fields(0).substring(1,fields(0).length()-1).toInt, fields(2))
			}).map(f => new countries(f._1, f._2))

		// Create the context with 1 file from the directory
		val lines = sc.textFile("s3n://mat-log-csv-prod/olap_log/log/prod/s3_tracking_log_csv/stat_clicks/1_12/883/2014/06/*.gz")

		//adv: CAMPAIGN_ID, PUBLISHER_ID, IS_UNIQUE, IOS_IFA, IOS_IFA_SHA1, USER_AGENT, WURFL_DEVICE_OS, WURFL_DEVICE_OS_VERSION,
		//PUBLISHER_SUB_PUBLISHER_ID, PUBLISHER_SUB_SITE_ID, PUBLISHER_SUB_CAMPAIGN_ID,COUNTRY_ID,  SITE_ID (13)
		val newtable = lines.map(line => {
				val fields = line.split("\t")
				fields
			}).filter(f=>f.length==155).filter( f => ( !parseDate(f(123).substring(1,f(123).length()-1)).get.before(parseDate("2014-06-01 00:00:00").get) && parseDate(f(123).substring(1,f(123).length()-1)).get.before(parseDate("2014-07-01 00:00:00").get)) ).map( f => (f(20), f(15), f(104), f(68), f(70), f(120), f(109), f(110), f(35), f(36), f(37), f(105), f(19)) ).filter(f=> isInt(f._1) && isInt(f._2) && isInt(f._9) && isInt(f._10) && isInt(f._11) && isInt(f._12)).map(f => new Clicks(f._1, f._2,f._3,f._4,f._5,f._6,f._7,f._8,f._9,f._10,f._11,f._12,f._13))

		val left = org.apache.spark.sql.catalyst.plans.LeftOuter
		val adv = newtable.as('adv)
		val c = join1key.as('c)
		val p = join2key.as('p)
		val ps = join3key.as('ps)
		val pss = join4key.as('pss)
		val psc = join5key.as('psc)
		val country = join6key.as('country)
		val result = adv.join(c,left,Some("adv.CAMPAIGN_ID".attr==="c.ID".attr)).join(p,left,Some("adv.PUBLISHER_ID".attr==="p.ID".attr)).join(ps,left,Some("adv.PUBLISHER_SUB_PUBLISHER_ID".attr==="ps.ID".attr)).join(pss,left,Some("adv.PUBLISHER_SUB_SITE_ID".attr==="pss.ID".attr)).join(psc,left,Some("adv.PUBLISHER_SUB_CAMPAIGN_ID".attr==="psc.ID".attr)).join(country,left,Some("adv.COUNTRY_ID".attr==="country.ID".attr)).select("c.NAME".attr, "p.NAME".attr, "adv.IS_UNIQUE".attr , "adv.IOS_IFA".attr , "adv.IOS_IFA_SHA1".attr, "adv.USER_AGENT".attr , "adv.WURFL_DEVICE_OS".attr , "adv.WURFL_DEVICE_OS_VERSION".attr , "ps.NAME".attr , "pss.NAME".attr , "psc.NAME".attr , "country.NAME".attr , "adv.SITE_ID".attr , "adv.CAMPAIGN_ID".attr , "adv.PUBLISHER_ID".attr , "adv.PUBLISHER_SUB_PUBLISHER_ID".attr , "adv.PUBLISHER_SUB_SITE_ID".attr , "adv.PUBLISHER_SUB_CAMPAIGN_ID".attr)
		result.saveAsTextFile("/mnt/result")


		println("time: "+(System.nanoTime-s)/1e6+"ms")
		sc.stop()
	}
}
