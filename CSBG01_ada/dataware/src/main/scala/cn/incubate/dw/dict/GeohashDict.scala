package www.beyondsoft.CSBG01_ada

import java.util.Properties

import ch.hsr.geohash.GeoHash
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GeohashDict {

  def main(args: Array[String]): Unit = {

    //create sparksession
    val spark: SparkSession = SparkSession.builder()
        .appName(this.getClass.getSimpleName)
        .master("local[*]")
        .getOrCreate()

    //read table tmp_mid from mysql
    val props: Properties = new Properties()
        props.setProperty("user", "ada")
        props.setProperty("password", "KqNdWZcZ")

    val dataFrame = spark.read.jdbc("jdbc:mysql://jm_pre_db_1_dev.t.lsctl.com:3307/carrier","tmp_mid",props)
    dataFrame.printSchema()
    dataFrame.show(10)
    import spark.sql
    var value: RDD[(String, String, String, String)] = dataFrame.rdd.map(row => {
      //get lat and lng
      val lat: Double = row.getAs[Double]("BD09_LAT")
      val lng: Double = row.getAs[Double]("BD09_LNG")
      val city: String = row.getAs[String]("city")
      val province: String = row.getAs[String]("province")
      val district: String = row.getAs[String]("district")
      val geoHash = GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 5)

      (geoHash, district, city, province)
    })
    // transfrom geo coordinates to Geoencoding



    spark.close()

  }
}
