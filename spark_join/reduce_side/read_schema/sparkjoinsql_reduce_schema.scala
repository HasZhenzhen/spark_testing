//after compilation, run with /root/spark/bin/spark-submit --class "SparkJoin2" --master spark://ec2-54-211-179-185.compute-1.amazonaws.com:7077 /root/zhenzhentest/schematest/target/scala-2.10/spark_join_sql_2.10-1.1.jar /root/zhenzhentest/schematest/dirFile /root/zhenzhentest/schematest/schemaFile
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import java.text.SimpleDateFormat
import scala.util.control.Exception.allCatch
import scala.io.Source

object SparkJoin2{
	def inTable(metaFile: String): List[String] = {
		try{
			Source.fromFile(metaFile).getLines.toList
			//for (line <- Source.fromFile(filename).getLines()) {
			//	Array(i) = line
			//	} 
			} catch {
				case ex: Exception => println("Bummer, an exception happened.")
				List()
			}
		}

	def createStructField(fieldNameType : String):StructField = {
    	val field= fieldNameType.split(":")
    	field(1) match {
      		case "INT" => StructField(field(0), IntegerType, true)
      		case "VARCHAR" => StructField(field(0), StringType, true)
      		case "DECIMAL" => StructField(field(0), DoubleType, true)
      		case "DATETIME" => StructField(field(0), StringType, true)
      		case default => StructField(field(0), StringType, true)
    	}
  	}

	def createSchema(schemaString : String) :StructType  = {
    	// Generate the schema based on the string of schema
    	val schema = StructType(schemaString.split(" ").map(fieldNameType => createStructField(fieldNameType)).toList)
    	schema
  		}
	

	def main(args: Array[String]) {
		if (args.length < 2) {
      		System.err.println("Usage: SparkJoin <indir> <inschema>")
      		System.exit(1)
    	}

    	val s = System.nanoTime
		val conf = new SparkConf().setAppName("Spark_Join_Sql")
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

		val dirs = inTable(args(0))
		val schemas = inTable(args(1))

		//val input1 = "s3n://mat-log-csv-prod/olap_log/join_table/prod/attribution_shared/campaigns/1_1/0/0/0/prod.campaigns.1_1.0.0.0.csv.gz"
		val input1 = dirs(0)
		val join1 = sc.textFile(input1)
		val join1RDD = join1.map(line => {
			val fields = line.split("\t")
			fields
			}).filter( f => !trimString(f(0)).isEmpty && f.length == 47 ).map(f => Row(trimString(f(0)).get.toInt, f(5)))
		// Apply the schema to the RDD.
		//val schemaStr1 = "ID:INT Name:VARCHAR"
		val schemaStr1 = schemas(0)
    	val schema1 = createSchema(schemaStr1)
    	val join1key = sqlContext.applySchema(join1RDD, schema1)

		//val input2 = "s3n://mat-log-csv-prod/olap_log/join_table/prod/attribution_shared/publishers/1_1/0/0/0/prod.publishers.1_1.0.0.0.csv.gz"
		val input2 = dirs(1)
		val join2 = sc.textFile(input2)
		val join2RDD = join2.map(line => {
			val fields = line.split("\t")
			Row(fields(0).substring(1,fields(0).length()-1).toInt, fields(6))
			})
		val schemaStr2 = schemas(1)
    	val schema2 = createSchema(schemaStr2)
    	val join2key = sqlContext.applySchema(join2RDD, schema2)

		//val input3 = "s3n://mat-log-csv-prod/olap_log/join_table/prod/attribution_shared/advertiser_sub_publishers/1_1/0/0/0/prod.advertiser_sub_publishers.1_1.0.0.0.csv.gz"
		val input3 = dirs(2)
		val join3 = sc.textFile(input3)
		val join3RDD = join3.map(line => {
			val fields = line.split("\t")
			Row(fields(0).substring(1,fields(0).length()-1).toInt, fields(2))
			})
		val schemaStr3 = schemas(2)
		val schema3 = createSchema(schemaStr3)
    	val join3key = sqlContext.applySchema(join3RDD, schema3)

		//val input4 = "s3n://mat-log-csv-prod/olap_log/join_table/prod/attribution_shared/advertiser_sub_sites/1_1/0/0/0/prod.advertiser_sub_sites.1_1.0.0.0.csv.gz"
		val input4 = dirs(3)
		val join4 = sc.textFile(input4)
		val join4RDD = join4.map(line => {
			val fields = line.split("\t")
			Row(fields(0).substring(1,fields(0).length()-1).toInt, fields(2))
			})
		val schemaStr4 = schemas(3)
		val schema4 = createSchema(schemaStr4)
    	val join4key = sqlContext.applySchema(join4RDD, schema4)

		//val input5 = "s3n://mat-log-csv-prod/olap_log/join_table/prod/attribution_shared/advertiser_sub_campaigns/1_1/0/0/0/prod.advertiser_sub_campaigns.1_1.0.0.0.csv.gz"
		val input5 = dirs(4)
		val join5 = sc.textFile(input5)
		val join5RDD = join5.map(line => {
			val fields = line.split("\t")
			fields}).filter(f => f.length == 8).map(f => Row(f(0).substring(1,f(0).length()-1).toInt, f(2)))
		val schemaStr5 = schemas(4)
		val schema5 = createSchema(schemaStr5)
    	val join5key = sqlContext.applySchema(join5RDD, schema5)

		//val input6 = "s3n://mat-log-csv-prod/olap_log/join_table/prod/attribution_shared/countries/1_1/0/0/0/prod.countries.1_1.0.0.0.csv.gz"
		val input6 = dirs(5)
		val join6 = sc.textFile(input6)
		val join6RDD = join6.map(line => {
			val fields = line.split("\t")
			Row(fields(0).substring(1,fields(0).length()-1).toInt, fields(2))
			})
		val schemaStr6 = schemas(5)
		val schema6 = createSchema(schemaStr6)
    	val join6key = sqlContext.applySchema(join6RDD, schema6)

		// Create the context with 1 file from the directory
		//val lines = sc.textFile("s3n://mat-log-csv-prod/olap_log/log/prod/s3_tracking_log_csv/stat_clicks/1_12/883/2014/06/*.gz")
		val lines = sc.textFile(dirs(6))

		//adv: CAMPAIGN_ID, PUBLISHER_ID, IS_UNIQUE, IOS_IFA, IOS_IFA_SHA1, USER_AGENT, WURFL_DEVICE_OS, WURFL_DEVICE_OS_VERSION,
		//PUBLISHER_SUB_PUBLISHER_ID, PUBLISHER_SUB_SITE_ID, PUBLISHER_SUB_CAMPAIGN_ID,COUNTRY_ID,  SITE_ID (13)
		val newtableRDD = lines.map(line => {
				val fields = line.split("\t")
				fields
			}).filter(f=>f.length==155).filter( f => ( !parseDate(f(123).substring(1,f(123).length()-1)).get.before(parseDate("2014-06-01 00:00:00").get) && parseDate(f(123).substring(1,f(123).length()-1)).get.before(parseDate("2014-07-01 00:00:00").get)) ).map( f => (f(20), f(15), f(104), f(68), f(70), f(120), f(109), f(110), f(35), f(36), f(37), f(105), f(19)) ).filter(f=> isInt(f._1) && isInt(f._2) && isInt(f._9) && isInt(f._10) && isInt(f._11) && isInt(f._12)).map(f => Row(f._1, f._2,f._3,f._4,f._5,f._6,f._7,f._8,f._9,f._10,f._11,f._12,f._13))
		val schemaStr7 = schemas(6)
		val schema7 = createSchema(schemaStr7)
    	val newtable = sqlContext.applySchema(newtableRDD, schema7)

		val left = org.apache.spark.sql.catalyst.plans.LeftOuter
		val adv = newtable.as('adv)
		val c = join1key.as('c)
		val p = join2key.as('p)
		val ps = join3key.as('ps)
		val pss = join4key.as('pss)
		val psc = join5key.as('psc)
		val country = join6key.as('country)
		val result = adv.join(c,left,Some("adv.CAMPAIGN_ID".attr==="c.ID".attr)).join(p,left,Some("adv.PUBLISHER_ID".attr==="p.ID".attr)).join(ps,left,Some("adv.PUBLISHER_SUB_PUBLISHER_ID".attr==="ps.ID".attr)).join(pss,left,Some("adv.PUBLISHER_SUB_SITE_ID".attr==="pss.ID".attr)).join(psc,left,Some("adv.PUBLISHER_SUB_CAMPAIGN_ID".attr==="psc.ID".attr)).join(country,left,Some("adv.COUNTRY_ID".attr==="country.ID".attr)).select("c.NAME".attr, "p.NAME".attr, "adv.IS_UNIQUE".attr , "adv.IOS_IFA".attr , "adv.IOS_IFA_SHA1".attr, "adv.USER_AGENT".attr , "adv.WURFL_DEVICE_OS".attr , "adv.WURFL_DEVICE_OS_VERSION".attr , "ps.NAME".attr , "pss.NAME".attr , "psc.NAME".attr , "country.NAME".attr , "adv.SITE_ID".attr , "adv.CAMPAIGN_ID".attr , "adv.PUBLISHER_ID".attr , "adv.PUBLISHER_SUB_PUBLISHER_ID".attr , "adv.PUBLISHER_SUB_SITE_ID".attr , "adv.PUBLISHER_SUB_CAMPAIGN_ID".attr)
		//result.saveAsTextFile("/mnt/result")
		result.saveAsTextFile(dirs(7))


		println("time: "+(System.nanoTime-s)/1e6+"ms")
		sc.stop()
	}
}
