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
val inpath = args(0)
val outpath = args(1)
val blockRow = (args(2).toInt / 100).toInt
val blockCol = args(3).toInt

println(args.mkString(","))

// conversion
val df = spark.read.options(Map("inferSchema"->"true", "header"->"false")).csv(inpath)
val indexeddf = df.rdd.zipWithIndex.map(x => new IndexedRow(x._2.toInt, new DenseVector(x._1.toSeq.toArray.map(_.toString.toDouble))));
val mat = new IndexedRowMatrix(indexeddf);
val bm = mat.toBlockMatrix(blockRow, blockCol);
bm.blocks.map(x => ((x._1._1.toString + "," + x._1._2.toString), doublearrayToBytearray(x._2.toArray))).saveAsSequenceFile(outpath)

// NOTE: below causes slow read performance when experiment
// bm.blocks.saveAsObjectFile(outpath)

sys.exit

