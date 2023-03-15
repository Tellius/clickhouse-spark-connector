
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.ZoneId
import java.time.format.DateTimeFormatter

import io.clickhouse.ext.ClickhouseConnectionFactory
import io.clickhouse.ext.spark.ClickhouseSparkExt._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest._

case class Row1(name: String, v: Int, v2: Int)

class TestSpec extends FlatSpec with Matchers {

  val sparkSession = SparkSession.builder
    .master("local")
    .appName("local spark")
    .getOrCreate()
  val db = "tmp1"
  val anyHost = "localhost"

  "case0" should "" in {

    val max = 25e6
    val monthSize = max / 11
    val daySize = monthSize / 28

    def yearMap(chrom: String) = {
      1900 + (math.abs(chrom.hashCode) % 200)
    }

    def monthDayMap(pos: Int) = {
      val m = (pos / monthSize).toInt
      val d = ((pos % monthSize) / daySize).toInt
      (m + 1, d + 1)
    }

    val r = (5024637 to 48119824).toList map { pos =>
      monthDayMap(pos)
    }

    val month_range = r.map(_._1).distinct
    val day_range = r.map(_._2).distinct

    assert(true)
  }

  "case 11" should "" in {

    val a = 1

    def calc(pos: Int) = {
      val x = pos / 25e6 * 348
      val m = x % 12
      val d = x % 29
      (m.toInt, d.toInt)
    }

    val r = (0 to 1000000).toList map { pos =>
      calc(pos)
    }

    val month_range = r.map(_._1).distinct
    val day_range = r.map(_._2).distinct


    assert(true)
  }

  "case1" should "ok" in {

    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    // test dframe
    val df = sqlContext.createDataFrame(1 to 10 map(i => Row1(s"$i", i, i + 10)) )

    // clickhouse params
    val db = "tmp1"
    val tableName = "t1"
//    val clusterName = None: Option[String]
    // start clickhouse docker using config.xml from clickhouse_files
    val clusterName = Some("perftest_1shards_1replicas"): Option[String]

    // define clickhouse connection
    implicit val clickhouseDataSource = ClickhouseConnectionFactory.get(anyHost, 8123)

    // create db / table
    df.dropClickhouseDb(db, clusterName)
    df.createClickhouseDb(db, clusterName)
    df.createClickhouseTable(db, tableName, "mock_date", Seq("name"), clusterNameO = clusterName)
    df.createClickhouseTable(db,tableName, clusterName)

    // save data
    val res = df.saveToClickhouse("tmp1", "t1", (row:Row) => java.sql.Date.valueOf("2000-12-01"), "mock_date", clusterNameO = clusterName)
    assert(res.size == 1)
    assert(res.get("localhost") == Some(df.count()))

    true should === (true)
  }

  "save df" should "store milli seconds precession" in {
    import sparkSession.implicits._

    // Sample data
    val data = Seq(
      (1, 2.5, "A", Timestamp.valueOf("2022-03-08 02:34:56.123456789")),
      (2, 3.7, "B", Timestamp.valueOf("2022-03-09 11:23:45.456789012")),
      (3, 1.2, "C", Timestamp.valueOf("2022-03-10 23:45:01")),
      (4, 5.5, "E", Timestamp.valueOf("2022-03-08 12:34:56.123456")),
      (5, 7.7, "F", Timestamp.valueOf("2022-03-09 01:23:45.456789")),
      (6, 8.2, "G", Timestamp.valueOf("2022-03-10 23:45:01"))
    )

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

    //here we are manipulating data to UTC as clickhouse **MIGHT BE** using UTC timeZone as a result given IST time are expected to be in GMT.
    val expectedTimeStamps = data.map { case (_, _, _, time) =>
      val localZoneId = ZoneId.systemDefault() //create a ZoneId object for LOCAL timezone
      val gmtZoneId = ZoneId.of("GMT") // create a ZoneId object for GMT timezone

      val localZonedDateTime = time.toLocalDateTime.atZone(localZoneId)
      val gmtZonedDateTime = localZonedDateTime.withZoneSameInstant(gmtZoneId)

      formatter.format(gmtZonedDateTime.toLocalDateTime()).format(formatter) // format the GMT timestamp as a string
    }

    val df = data.toDF("index", "measure", "dimension", "time_up_to_nano_precession")
    val tableName = "store_date_with_micro_sec"
    implicit val clickhouseDataSource = ClickhouseConnectionFactory.get(anyHost, 8123)

    df.dropClickhouseDb(db)
    df.createClickhouseDb(db)
    df.createClickhouseTable(db, tableName)

    val res = df.saveToClickhouse(db, tableName, 1, None)
    res.getOrElse(anyHost, -1) shouldBe(6)
    val conn = clickhouseDataSource.getConnection
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(s"select * from $db.${tableName}")
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    while (rs.next()) {
      val currentIndex = rs.getInt("index") - 1
      val current = data.apply(currentIndex)
      current._1 shouldBe(currentIndex + 1)
      current._2 shouldBe(rs.getDouble("measure"))
      current._3 shouldBe(rs.getString("dimension"))
      expectedTimeStamps.apply(currentIndex) shouldBe rs.getString("time_up_to_nano_precession")
    }

    // Close the JDBC connection and release resources
    rs.close()
    stmt.close()
    conn.close()

  }
}
