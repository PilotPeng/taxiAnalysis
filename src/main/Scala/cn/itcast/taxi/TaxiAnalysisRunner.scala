package cn.itcast.taxi

import com.esri.core.geometry.{GeometryEngine, Point, SpatialReference}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.text.SimpleDateFormat
import java.util.Locale
import java.util.concurrent.TimeUnit
import scala.io.Source

object TaxiAnalysisRunner {

  def main(args: Array[String]): Unit = {
    // 1. 创建SparkSession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("taxi")
      .getOrCreate()

    // 2. 导入隐式转换和函数
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 3. 数据读取
    val taxiRaw: DataFrame = spark.read
      .option("header", value = true)
      .csv("dataset/half_trip.csv")

//    taxiRaw.show(10)
//    taxiRaw.printSchema()

    // 4. 转换操作
    val taxiParsed: RDD[Either[Trip, (Row, Exception)]] = taxiRaw.rdd.map(safe(parse))
    // 可以通过如下方式来过滤出来所有异常的row
//    val result: RDD[Row] = taxiParsed.filter(e => e.isRight)
//      .map(e => e.right.get._1)  // 发生错误的Row类型的对象

    val taxiGood: Dataset[Trip] = taxiParsed.map(either => either.left.get ).toDS()
//    taxiGood.show()

    // 5. 绘制时长直方图
    // 5.1 编写UDF完成时长计算，将毫秒转为小时单位
    val hours = (pickUpTime: Long, dropOffTime: Long) => {
      val duration = dropOffTime - pickUpTime
      val hours = TimeUnit.HOURS.convert(duration, TimeUnit.MILLISECONDS)
      hours
    }
    val hoursUDF = udf(hours)

    // 5.2 进行统计
    taxiGood.groupBy(hoursUDF($"pickUpTime", $"dropOffTime") as "duration")
      .count()
      .sort("duration")
//      .show()

    // 6. 根据直方图的显示，查看数据分布后，剪除反常数据
    spark.udf.register("hours", hours)
    val taxiClean = taxiGood.where("hours(pickUpTime, dropOffTime) BETWEEN 0 AND 3")
//    taxiClean.show()

    // 7. 增加行政区信息
    // 7.1 读取数据集
    val geoJson = Source.fromFile("dataset/nyc-borough-boundaries-polygon.geojson").mkString
    val featureCollection = FeatureExtraction.parseJson(geoJson)
    // 7.2 排序
    // 后续需要得到每一个出租车所在的行政区，拿到经纬度，遍历features搜索其所在的行政区
    // 在搜索的过程中，行政区越大命中的几率就越高，所以把大的行政区放在前面，更容易命中，减少遍历次数
    val sortedFeatures = featureCollection.features.sortBy(feature => {
      (feature.properties("boroughCode"), - feature.getGeometry().calculateArea2D())
    })
    // 7.3 广播
    val featuresBC = spark.sparkContext.broadcast(sortedFeatures)
    // 7.4 UDF创建，完成功能
    val boroughLookUp = (x: Double, y: Double) => {
      // 7.4.1 搜索经纬度所在的行政区
      val featureHit: Option[Feature] = featuresBC.value.find(feature => {
        GeometryEngine.contains(feature.getGeometry(), new Point(x, y), SpatialReference.create(4326))
      })
      // 7.4.2 转为行政区信息
      val borough = featureHit.map(feature => feature.properties("borough")).getOrElse("NA")
      borough
    }
    // 7.5 统计信息
//    val boroughUDF = udf(boroughLookUp)
//    taxiClean.groupBy(boroughUDF('dropOffX, 'dropOffY))
//      .count()
//      .show()

    // 8.1 过滤没有经纬度的数据
    // 8.2 会话分析
    val sessions = taxiClean.where("dropOffX != 0 and dropOffY != 0 and pickUpX != 0 and pickUpY != 0")
      .repartition('licence)
      .sortWithinPartitions('licence, 'pickUpTime)

    // 8.3 求得前后两条数据的时间差
    def boroughDuration(t1: Trip, t2: Trip): (String, Long) = {
      val borough = boroughLookUp(t1.dropOffX, t1.dropOffY)
      val duration = (t2.pickUpTime - t1.dropOffTime) / 1000
      (borough, duration)
    }

    val boroughDu = sessions.mapPartitions(trips => {
      val viter = trips.sliding(2)           // 窗口为2
        .filter(_.size == 2)     // 窗口为偶数，即2
        .filter(p => p.head.licence == p.last.licence)  // 同一辆出租车
      viter.map(p => boroughDuration(p.head, p.last))
    }).toDF("borough", "seconds")

    boroughDu.where("seconds > 0")
      .groupBy("borough")
      .agg(avg('seconds), stddev('seconds))
      .show()


  }

  /**
   * 封装parse方法，捕获异常
   */
  def safe[P, R](f: P => R): P => Either[R, (P, Exception)] = {
    new Function[P, Either[R, (P, Exception)]] with Serializable {

      override def apply(param: P): Either[R, (P, Exception)] = {
        try {
          Left(f(param))
        } catch {
          case e: Exception => Right((param, e))
        }
      }
    }
  }

  /**
   * Row -> Trip
   * @param row
   * @return
   */
  def parse(row: Row): Trip ={
    val richRow = new RichRow(row)
    val licence = richRow.getAs[String]("hack_license").orNull  // 空值为null
    val pickUpTime = parseTime(richRow, "pickup_datetime")
    val dropOffTime = parseTime(richRow, "dropoff_datetime")
    val pickUpX = parseLocation(richRow, "pickup_longitude")
    val pickUpY = parseLocation(richRow, "pickup_latitude")
    val dropOffX = parseLocation(richRow, "dropoff_longitude")
    val dropOffY = parseLocation(richRow, "dropoff_latitude")
    Trip(licence, pickUpTime, dropOffTime, pickUpX, pickUpY, dropOffX, dropOffY)
  }

  /**
   * 将时间类型转为时间戳
   * @param row 行数据，类型为RichRow，以便处理空值
   * @param field 要处理的时间字段所在的位置
   * @return 返回Long型的时间戳
   */
  def parseTime(row: RichRow, field: String): Long = {
    // 1. 表示出来时间类型的格式 SimpleDateFormat
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val formatter = new SimpleDateFormat(pattern, Locale.ENGLISH)

    // 2. 执行转换，获取Date对象，getTime获取时间戳
    val time: Option[String] = row.getAs[String](field)
    val timeOption: Option[Long] = time.map(item => formatter.parse(item).getTime)
    timeOption.getOrElse(0L)

  }

  /**
   * 将字符串标识的Double数据转为Double类型的对象
   * @param row
   * @param field
   * @return
   */
  def parseLocation(row: RichRow, field: String): Double = {
    // 1. 获取数据
    val location: Option[String] = row.getAs[String](field)

    // 2. 转换数据
    val locationOption: Option[Double] = location.map(item => item.toDouble)
    locationOption.getOrElse(0.0D)
  }

}

/**
 * DataFrame中的Row的包装类型，主要为了包装getAs方法
 * @param row
 */
class RichRow(row: Row) {
  /**
   * 为了返回option，提醒外面处理空值，提供处理方式
   * @param field
   * @tparam T
   * @return
   */
  def getAs[T](field: String): Option[T] = {
    // 1. 判断row.getAs是否为空，row中对应的field是否为空
    if (row.isNullAt(row.fieldIndex(field))) {
      // 2. null 返回None
      None
    } else {
      // 3. not null 返回Some
      Some(row.getAs[T](field))
    }
  }
}

/**
 * 一个Trip对象代表一个出租车行程
 * @param licence
 * @param pickUpTime
 * @param dropOffTime
 * @param pickUpX
 * @param pickUpY
 * @param dropOffX
 * @param dropOffY
 */
case class Trip (
      licence: String,
      pickUpTime: Long,
      dropOffTime: Long,
      pickUpX: Double,
      pickUpY: Double,
      dropOffX: Double,
      dropOffY: Double
)





















