import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
//import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

object ReturnTrips {
  def compute(trips : Dataset[Row], dist : Double, spark : SparkSession) : Dataset[Row] = {

    import spark.implicits._
    //import sqlContext.implicits._

    spark.udf.register("distance", (startLongitude: Double,startLatitude: Double,
                                    endLongitude: Double, endLatitude: Double) => calculateDistanceInKilometer(startLongitude,startLatitude,
      endLongitude,endLatitude)) //TODO: replace 50
    spark.udf.register("add_hours", (datetime : Timestamp, hours : Int) => {
      new Timestamp(datetime.getTime() + hours * 60 * 60 * 1000 )
    });

    //trips.write.bucketBy()
    //val check = calculateBucketEndPointLat(-73.9818420410156,100)
    //val check2 = calculateBucketEndPointLng(-73.9818420410156,40.73240662,100)
    //println(check)
    //println(check2)
    val ColumnNames_pick = Seq("tpep_pickup_datetime","tpep_dropoff_datetime",
      "pickup_longitude","pickup_latitude",
      "dropoff_longitude","dropoff_latitude","Lat_bucket_pick","Lng_bucket_pick","Time_bucket_drop")
    val ColumnNames_drop = Seq("tpep_pickup_datetime","tpep_dropoff_datetime",
      "pickup_longitude","pickup_latitude",
      "dropoff_longitude","dropoff_latitude","Lat_bucket_drop","Lng_bucket_drop","Time_bucket_pick")

    //val ColumnNames_drop_rename = Seq("tpep_pickup_datetime_drop","tpep_dropoff_datetime_drop",
    //  "pickup_longitude_drop","pickup_latitude_drop",
    //  "dropoff_longitude_drop","dropoff_latitude_drop","Lat_bucket_drop","Lng_bucket_drop")



    //val MaxPicLng = trips.agg(max(trips.col("pickup_longitude"))).head().get(0).toString().toDouble
    //val MaxdropLng = trips.agg(max(trips.col("dropoff_longitude"))).head().get(0).toString().toDouble
    //val MaxLng = Math.max(MaxPicLng,MaxdropLng)


    val MinPicLng = trips.agg(min(trips.col("pickup_longitude"))).head().get(0).toString().toDouble
    val MinDropLng = trips.agg(min(trips.col("dropoff_longitude"))).head().get(0).toString().toDouble
    val MinLng = Math.min(MinPicLng,MinDropLng)

    //println("Min Lontitude")
    //println(MinPicLng)
    //println(MinDropLng)
    //println(MinLng)

    val MaxPicLat = trips.agg(max(trips.col("pickup_latitude"))).head().get(0).toString().toDouble
    //val MaxDropLat = trips.agg(max(trips.col("pickup_latitude"))).head().get(0).toString().toDouble
    val MaxDropLat = trips.agg(max(trips.col("dropoff_latitude"))).head().get(0).toString().toDouble
    val MaxLat = Math.max(MaxPicLat,MaxDropLat)

    //println("Max Latitude")
    //println(MaxPicLat)
    //println(MaxDropLat)
    //println(MaxLat)

    val MinPicLat = trips.agg(min(trips.col("pickup_latitude"))).head().get(0).toString().toDouble
    val MinDropLat = trips.agg(min(trips.col("dropoff_latitude"))).head().get(0).toString().toDouble
    val MinLat = Math.min(MinPicLat,MinDropLat)

    //println("Min Latitude")
    //println(MinPicLat)
    //println(MinDropLat)
    //println(MinLat)

    //val MinLat = MinLat_t - 0.0009 * dist /100
    //val MinLng = MinLng_t - 0.00355 * dist / 100
    //val TotalDisLat = calculateDistanceInKilometer(MinLng,MinLat,MinLng,MaxLat)
    //val BucketSizeLat = Math.ceil(TotalDisLat/dist).toInt
    //val BucketSizeLat =  0.0009 * dist /100
    val nextLat = calculateBucketEndPointLat(MinLat,dist)
   // println(nextLat)
    //println(MinLat)
    val BucketSizeLatStep = Math.abs(nextLat - MinLat)
    //println(1/BucketSizeLatStep)
    //val BucketSizeLatStep =  0.0009 * dist /100
    //val TotalDisLng = calculateDistanceInKilometer(MinLng,MaxLat,MaxLng,MaxLat)
    //val BucketSizeLng = Math.ceil(TotalDisLng/dist).toInt
    //val BucketSizeLng = 0.00355 * dist / 100

    val nextLng = calculateBucketEndPointLng(MaxLat,MinLng,dist)

    //println(nextLng)
    //println(MinLng)
    val BucketSizeLngStep = Math.abs(nextLng - MinLng)
    //println(1/BucketSizeLngStep)


    //val BucketSizeLngStep = 0.00355 * dist / 100

    //println("bucketsize")
    //println(BucketSizeLat)
    //println(BucketSizeLng)
    val tripsLatPickBucked = trips.withColumn("Lat_bucket_pick",
      //ceil(($"pickup_latitude"-MinLat)/(MaxLat-MinLat)*BucketSizeLatStep))
      ceil(($"pickup_latitude")/BucketSizeLatStep))

    val tripsLngPickBucked = tripsLatPickBucked.withColumn("Lng_bucket_pick",
      //ceil(($"pickup_longitude"-MinLng)/(MaxLng-MinLng)*BucketSizeLngStep))
      ceil(($"pickup_longitude")/BucketSizeLngStep))

/*    val tripsLngPickBucked_PickTime = tripsLngPickBucked
      .withColumn("Time_bucket_pick",ceil(($"tpep_pickup_datetime"/(8*60*60))))*/
    val tripsLngPickBucked_DropTime = tripsLngPickBucked
      .withColumn("Time_bucket_drop",ceil(($"tpep_dropoff_datetime".cast(DataTypes.LongType)/(8*60*60))))

    //val tripsLatPickBucked_ex = tripsLngPickBucked.withColumn("Lat_bucket_pick",
    //  explode(array($"Lat_bucket_pick" - 1, $"Lat_bucket_pick", $"Lat_bucket_pick" + 1)))

    //val tripsLngPickBucked_ex = tripsLatPickBucked_ex.withColumn("Lng_bucket_pick",
    //  explode(array($"Lng_bucket_pick" - 1, $"Lng_bucket_pick", $"Lng_bucket_pick" + 1)))

    val tripsPick = tripsLngPickBucked_DropTime.select(ColumnNames_pick.head, ColumnNames_pick.tail: _*)
    //val tripsPick = tripsLngPickBucked_ex.select(ColumnNames_pick.head, ColumnNames_pick.tail: _*)

    val tripsLatDropBucked = trips.withColumn("Lat_bucket_drop",
      //ceil(($"dropoff_latitude"-MinLat)/(MaxLat-MinLat)*BucketSizeLatStep))
      ceil(($"dropoff_latitude")/BucketSizeLatStep))

    val tripsLngDropBucked = tripsLatDropBucked.withColumn("Lng_bucket_drop",
      //ceil(($"dropoff_longitude"-MinLng)/(MaxLng-MinLng)*BucketSizeLngStep))
      ceil(($"dropoff_longitude")/BucketSizeLngStep))

    val tripsLngDropBucked_PickTime = tripsLngDropBucked
          .withColumn("Time_bucket_pick",ceil(($"tpep_pickup_datetime".cast(DataTypes.LongType)/(8*60*60))))

    val tripsLatDropBucked_ex = tripsLngDropBucked_PickTime.withColumn("Lat_bucket_drop",
      explode(array($"Lat_bucket_drop" - 1, $"Lat_bucket_drop", $"Lat_bucket_drop" + 1)))

    val tripsLngDropBucked_ex = tripsLatDropBucked_ex.withColumn("Lng_bucket_drop",
      explode(array($"Lng_bucket_drop" - 1, $"Lng_bucket_drop", $"Lng_bucket_drop" + 1)))

    val tripsLngDropBucked_ex_Time = tripsLngDropBucked_ex.withColumn("Time_bucket_pick",
      explode(array($"Time_bucket_pick" - 1, $"Time_bucket_pick")))

    val tripsDrop1 = tripsLngDropBucked_ex_Time.select(ColumnNames_drop.head, ColumnNames_drop.tail: _*)

    val tripsDrop = tripsDrop1
      .withColumnRenamed("tpep_pickup_datetime","tpep_pickup_datetime_2")
      .withColumnRenamed("tpep_dropoff_datetime","tpep_dropoff_datetime_2")
      .withColumnRenamed("pickup_longitude","pickup_longitude_drop")
      .withColumnRenamed("pickup_latitude","pickup_latitude_drop")
      .withColumnRenamed("dropoff_longitude","dropoff_longitude_drop")
      .withColumnRenamed("dropoff_latitude","dropoff_latitude_drop")

    //val ColumnNames_drop_rename = Seq("tpep_pickup_datetime_drop","tpep_dropoff_datetime_drop",
    //  "pickup_longitude_drop","pickup_latitude_drop",
    //  "dropoff_longitude_drop","dropoff_latitude_drop","Lat_bucket_drop","Lng_bucket_drop")
    val trip_join = tripsPick.as("a").join(tripsDrop.as("b"),
      ($"a.Lat_bucket_pick"===$"b.Lat_bucket_drop")
        &&($"a.Lng_bucket_pick"===$"b.Lng_bucket_drop")
        &&($"a.Time_bucket_drop"===$"b.Time_bucket_pick")

    )

    //trip_join.printSchema()
    //trip_join.show(2)

    //trip_join.createTempView("trip") // DataSet into Table 1
    //trip_join.createTempView("trip2") // DataSet into Table 2

    val trip_temp = trip_join
      .filter("tpep_dropoff_datetime < tpep_pickup_datetime_2")
      .filter("add_hours(tpep_dropoff_datetime, 8) > tpep_pickup_datetime_2")
      .filter("distance(dropoff_longitude, dropoff_latitude, pickup_longitude_drop, pickup_latitude_drop) <" + dist)
      .filter("distance(dropoff_longitude_drop, dropoff_latitude_drop, pickup_longitude, pickup_latitude) <" + dist)

      //.filter(abs($"pickup_longitude" - $"dropoff_longitude_drop") < lit(BucketSizeLngStep))
      //.filter(abs($"pickup_latitude" - $"dropoff_latitude_drop") < lit(BucketSizeLatStep))
      //.filter(abs($"pickup_longitude_drop" - $"dropoff_longitude") < lit(BucketSizeLngStep))
      //.filter(abs($"pickup_latitude_drop" - $"dropoff_latitude") < lit(BucketSizeLatStep) )

    val trip_last = trip_temp
/*    val trip_temp2 = trip_temp.drop("Lat_bucket_pick").drop("Lat_bucket_drop")
      .drop("Lng_bucket_pick").drop("Lng_bucket_drop")
    val trip_last = trip_temp2.distinct()*/


    //val query = "SELECT * FROM trip AS a where " +
    //  "distance(a.dropoff_longitude, a.dropoff_latitude, a.pickup_longitude_drop, a.pickup_latitude_drop) <" + dist + " and " +
    //  "distance(a.dropoff_longitude_drop, a.dropoff_latitude_drop, a.pickup_longitude, a.pickup_latitude) <" + dist + " and " +
    //  "a.tpep_dropoff_datetime < a.tpep_pickup_datetime_2 and " +
    //  "add_hours(a.tpep_dropoff_datetime, 8) > a.tpep_pickup_datetime_2"

    //spark.sql(query) // return directly


    //import spark.implicits._
    //var trip_check = trips
    //trips.printSchema()
    //trips.show(numRows = 2)
    //val tm = trips.select("total_amount")
    //val pt = trip_check.select("tpep_pickup_datetime")
    //val dt = trip_check.select("tpep_dropoff_datetime")

    //pt.show(2)
    //dt.show(2)

    //printf("your value is %s".tm)
    //printf("your value is %s".tm.rdd.map(_(0).asInstanceOf[Int]).reduce(_+_))

    //trips.schema.fields.foreach(println(trips.col(colName = "total_amount")))
    //trips.join(trip_check,trips.col(colName = "VendorID"))

    //println("Paul")
    trip_last
  }

  //case class Location(lat: Double, lon: Double)

  /*def calculateMaxLat (tripA: Location, dist : Double): Double = {


  }*/
  def calculateDistanceInKilometer(startLongitude: Double,startLatitude: Double,
                                   endLongitude: Double, endLatitude: Double): Double = {
    val AVERAGE_RADIUS_OF_EARTH_KM = 6371
    val latDistance = Math.toRadians(startLatitude - endLatitude)
    val lngDistance = Math.toRadians(startLongitude - endLongitude)
    val sinLat = Math.sin(latDistance / 2)
    val sinLng = Math.sin(lngDistance / 2)
    val a = sinLat * sinLat +
      (Math.cos(Math.toRadians(startLatitude))
        * Math.cos(Math.toRadians(endLatitude))
        *sinLng * sinLng)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    AVERAGE_RADIUS_OF_EARTH_KM  *c*  1000
  }


  def calculateBucketEndPointLat(startLatitude:Double,dist:Double): Double ={
    val AVERAGE_RADIUS_OF_EARTH_KM = 6371
    val c = dist / (1000 * AVERAGE_RADIUS_OF_EARTH_KM)
    val a = Math.pow(Math.tan(c/2),2) /(Math.pow(Math.tan(c/2),2)+1)
    val sinLat = Math.sqrt(a)
    val latDistance = 2 * Math.asin(sinLat)
    startLatitude - Math.toDegrees(latDistance)
  }

  def calculateBucketEndPointLng(startLatitude:Double,startLongitude:Double,dist:Double): Double ={
    val AVERAGE_RADIUS_OF_EARTH_KM = 6371
    val c = dist / (1000 * AVERAGE_RADIUS_OF_EARTH_KM)
    val a = Math.pow(Math.tan(c/2),2) /(Math.pow(Math.tan(c/2),2)+1)
    val sinLng = Math.sqrt(a)/(Math.cos(Math.toRadians(startLatitude))
      * 1)
    val lngDistance = 2 * Math.asin(sinLng)
    startLongitude - Math.toDegrees(lngDistance)
  }



}

