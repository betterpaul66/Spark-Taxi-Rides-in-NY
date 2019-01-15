package taxi


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


    val ColumnNames_pick = Seq("tpep_pickup_datetime","tpep_dropoff_datetime",
      "pickup_longitude","pickup_latitude",
      "dropoff_longitude","dropoff_latitude","Lat_bucket_pick","Lng_bucket_pick","Time_bucket_drop")
    val ColumnNames_drop = Seq("tpep_pickup_datetime","tpep_dropoff_datetime",
      "pickup_longitude","pickup_latitude",
      "dropoff_longitude","dropoff_latitude","Lat_bucket_drop","Lng_bucket_drop","Time_bucket_pick")//just select the columns we need

    val MinPicLng = trips.agg(min(trips.col("pickup_longitude"))).head().get(0).toString().toDouble
    val MinDropLng = trips.agg(min(trips.col("dropoff_longitude"))).head().get(0).toString().toDouble
    val MinLng = Math.min(MinPicLng,MinDropLng) //find the minimum longitude

    val MaxPicLat = trips.agg(max(trips.col("pickup_latitude"))).head().get(0).toString().toDouble
    val MaxDropLat = trips.agg(max(trips.col("dropoff_latitude"))).head().get(0).toString().toDouble
    val MaxLat = Math.max(MaxPicLat,MaxDropLat) //find the maximum latitude

    val MinPicLat = trips.agg(min(trips.col("pickup_latitude"))).head().get(0).toString().toDouble
    val MinDropLat = trips.agg(min(trips.col("dropoff_latitude"))).head().get(0).toString().toDouble
    val MinLat = Math.min(MinPicLat,MinDropLat) //find the minimum latitude


    val nextLat = calculateBucketEndPointLat(MinLat,dist)  //use the minimum lat to find next latitude by given distance

    val BucketSizeLatStep = Math.abs(nextLat - MinLat) // bucket size of latitude

    val nextLng = calculateBucketEndPointLng(MaxLat,MinLng,dist) //use the minimum lon to find next lon by given distance,and min lat. Since log calculation has one more cosine term on lat. use the min lat to maximize the lon bucket size

    val BucketSizeLngStep = Math.abs(nextLng - MinLng) // bucket size of longitude

    val tripsLatPickBucked = trips.withColumn("Lat_bucket_pick",
      ceil(($"pickup_latitude")/BucketSizeLatStep)) //calculate the number of bucket from current pick lat to min lat

    val tripsLngPickBucked = tripsLatPickBucked.withColumn("Lng_bucket_pick",
      ceil(($"pickup_longitude")/BucketSizeLngStep)) //calculate the number of bucket from current pick lon to min lon


    val tripsLngPickBucked_DropTime = tripsLngPickBucked
      .withColumn("Time_bucket_drop",ceil(($"tpep_dropoff_datetime".cast(DataTypes.LongType)/(8*60*60))))
    //calculate the number of bucket of drop time

    val tripsPick = tripsLngPickBucked_DropTime.select(ColumnNames_pick.head, ColumnNames_pick.tail: _*)

    val tripsLatDropBucked = trips.withColumn("Lat_bucket_drop",
      ceil(($"dropoff_latitude")/BucketSizeLatStep))
    //calculate the number of bucket from current drop lat to min lat

    val tripsLngDropBucked = tripsLatDropBucked.withColumn("Lng_bucket_drop",
      ceil(($"dropoff_longitude")/BucketSizeLngStep))
    //calculate the number of bucket from current dropoff lon to min lat

    val tripsLngDropBucked_PickTime = tripsLngDropBucked
          .withColumn("Time_bucket_pick",ceil(($"tpep_pickup_datetime".cast(DataTypes.LongType)/(8*60*60))))
    //calculate the number of bucket of drop time

    val tripsLatDropBucked_ex = tripsLngDropBucked_PickTime.withColumn("Lat_bucket_drop",
      explode(array($"Lat_bucket_drop" - 1, $"Lat_bucket_drop", $"Lat_bucket_drop" + 1)))
    //extend the bucket to 3 for later joining


    val tripsLngDropBucked_ex = tripsLatDropBucked_ex.withColumn("Lng_bucket_drop",
      explode(array($"Lng_bucket_drop" - 1, $"Lng_bucket_drop", $"Lng_bucket_drop" + 1)))
    //extend the bucket to 3 for later joining

    val tripsLngDropBucked_ex_Time = tripsLngDropBucked_ex.withColumn("Time_bucket_pick",
      explode(array($"Time_bucket_pick" - 1, $"Time_bucket_pick")))
    //extend the bucket to 2 for later joining

    val tripsDrop1 = tripsLngDropBucked_ex_Time.select(ColumnNames_drop.head, ColumnNames_drop.tail: _*)

    val tripsDrop = tripsDrop1
      .withColumnRenamed("tpep_pickup_datetime","tpep_pickup_datetime_2")
      .withColumnRenamed("tpep_dropoff_datetime","tpep_dropoff_datetime_2")
      .withColumnRenamed("pickup_longitude","pickup_longitude_drop")
      .withColumnRenamed("pickup_latitude","pickup_latitude_drop")
      .withColumnRenamed("dropoff_longitude","dropoff_longitude_drop")
      .withColumnRenamed("dropoff_latitude","dropoff_latitude_drop")


    val trip_join = tripsPick.as("a").join(tripsDrop.as("b"),
      ($"a.Lat_bucket_pick"===$"b.Lat_bucket_drop")
        &&($"a.Lng_bucket_pick"===$"b.Lng_bucket_drop")
        &&($"a.Time_bucket_drop"===$"b.Time_bucket_pick")

    )
    // join the table


    val trip_temp = trip_join
      .filter("tpep_dropoff_datetime < tpep_pickup_datetime_2") //should filter the time first which will be much faster
      .filter("add_hours(tpep_dropoff_datetime, 8) > tpep_pickup_datetime_2")
      .filter("distance(dropoff_longitude, dropoff_latitude, pickup_longitude_drop, pickup_latitude_drop) <" + dist)
      .filter("distance(dropoff_longitude_drop, dropoff_latitude_drop, pickup_longitude, pickup_latitude) <" + dist)
      //filter the table


    val trip_last = trip_temp

    trip_last
  }


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

