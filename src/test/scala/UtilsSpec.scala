import java.util.Properties

import org.scalatest.{FlatSpec, Matchers}
import io.clickhouse.ext.Utils._

class UtilsSpec extends FlatSpec with Matchers{

  "case 1" should "ok" in {

    var f = false
    case class Mock(){
      def print(): Unit = {
        println("mock print")
      }
      def close(): Unit ={
        f = true
      }
    }

    using(Mock()){ mock =>
      mock.print()
    }
    assert(f.equals(true))
  }

  "bucketise" should "create buckets" in {
    val buckets1 = bucketise(224, 25)
    buckets1.head shouldEqual (0, 25)
    buckets1.last shouldEqual (200, 224)
    buckets1.length shouldEqual 9

    val buckets2 = bucketise(103, 25)
    buckets2.head shouldEqual (0, 25)
    buckets2.last shouldEqual (100, 103)
    buckets2.length shouldEqual 5

    val buckets3 = bucketise(22, 25)
    buckets3.head shouldEqual (0, 22)
    buckets3.last shouldEqual (0, 22)
    buckets3.length shouldEqual 1

    val buckets4 = bucketise(22, 22)
    buckets3.head shouldEqual (0, 22)
    buckets3.last shouldEqual (0, 22)
    buckets3.length shouldEqual 1
  
    val buckets5 = bucketise(100, 25)
    buckets5.head shouldEqual (0, 25)
    buckets5.last shouldEqual (75, 100)
    buckets5.length shouldEqual 4
  }

}
