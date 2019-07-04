package io.clickhouse.ext

object Utils {
  

  def using[A, B <: {def close(): Unit}] (closeable: B) (f: B => A): A =
    try {
      f(closeable)
    }
    finally {
      closeable.close()
    }

  def bucketise(maxValue:Int, size:Int):List[(Int, Int)] = {
    if(size > maxValue) return List((0, maxValue))
    val values = (0 to maxValue).toList
    val rangeBoundaries = (0 to maxValue by size).toList
    val lastRange = if (rangeBoundaries.max < maxValue)
      List(List(rangeBoundaries.max, maxValue))
    else
      List.empty[List[Int]]
    val ranges = rangeBoundaries.sliding(2,1).toList ++ lastRange
    ranges.map(range => {
      (range(0), range(1))  
    })
  }
}
