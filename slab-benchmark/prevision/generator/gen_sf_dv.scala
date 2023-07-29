import java.nio.ByteBuffer

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed._

def bytearrayToDoublearray(bytearray: Array[Byte]) : Array[Double] = {
  val size = bytearray.length / 8
  var res = new Array[Double](size)
  for (i <- 0 until size) {
    res(i) = ByteBuffer.wrap(bytearray, i * 8, 8).getDouble()
  }

  res
}

def doublearrayToBytearray(doublearray: Array[Double]) : Array[Byte] = {
  val size = doublearray.length
  var res = new Array[Byte](size * 8)
  for (i <- 0 until size) {
    ByteBuffer.wrap(res, i * 8, 8).putDouble(doublearray(i))
  }

  res
}

val args = sc.getConf.get("spark.driver.args").split("\\s+")
val outpath = args(0)
val arrside = args(1).toInt
val tileside = args(2).toInt

println(args.mkString(","))

// conversion
val value = 1.toDouble / arrside
var bm = new BlockMatrix(sc.parallelize((0 to 9)).map{ x => 
  // println(value)
  if (x == 9) {
    val side = arrside % tileside
    val input = Array.fill[Double](side)(value) ++ Array.fill[Double](tileside - side)(0)
    println(input.length)
    println(input(side-1))
    println(input(side))
    ((x, 0), Matrices.dense(tileside, 1, input))
  } else {
    ((x, 0), Matrices.dense(tileside, 1, Array.fill[Double](tileside)(value)))
  }
}, tileside, 1)
bm.blocks.map(x => ((x._1._1.toString + "," + x._1._2.toString), doublearrayToBytearray(x._2.toArray))).saveAsSequenceFile(outpath)


// NOTE: below causes slow read performance when experiment
// bm.blocks.saveAsObjectFile(outpath)

sys.exit

