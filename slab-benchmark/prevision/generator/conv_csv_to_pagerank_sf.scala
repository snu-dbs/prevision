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

def bytearrayToIntarray(bytearray: Array[Byte]) : Array[Int] = {
  val size = bytearray.length / 4
  var res = new Array[Int](size)
  for (i <- 0 until size) {
    res(i) = ByteBuffer.wrap(bytearray, i * 4, 4).getInt()
  }

  res
}

def intarrayToBytearray(intarray: Array[Int]) : Array[Byte] = {
  val size = intarray.length
  var res = new Array[Byte](size * 4)
  for (i <- 0 until size) {
    ByteBuffer.wrap(res, i * 4, 4).putInt(intarray(i))
  }

  res
}

val args = sc.getConf.get("spark.driver.args").split("\\s+")
val inpath = args(0)
val outpath = args(1)
val blockRow = (args(2).toInt / 10).toInt
val blockCol = (args(2).toInt / 10).toInt

println(args.mkString(","))

// CSV -> BlockMatrix
val csv_before = spark.read.options(Map("header"->"false", "delimiter"->"\t")).csv(inpath)
val csv = csv_before.union(spark.createDataFrame(Seq((0, 0, 0), (args(2).toInt - 1, args(2).toInt - 1, 0))))
val coo = csv.rdd.map(x => ((x.getString(0).toInt, x.getString(1).toInt), x.getString(2).toDouble))
val entries = coo.map(x => new MatrixEntry(x._1._1, x._1._2, x._2))
val mat = new CoordinateMatrix(entries)
val bmat = mat.toBlockMatrix(blockRow, blockCol)

// Write BlockMatrix
bmat.blocks.map{ x => 
  val sm = x._2.asInstanceOf[SparseMatrix]
  val key = x._1._1.toString + "," + x._1._2.toString
  print(sm.colPtrs.length)
  print(" ")
  print(sm.rowIndices.length)
  print(" ")
  println(sm.values.length)
  val meta = intarrayToBytearray(Array(sm.colPtrs.length, sm.rowIndices.length, sm.values.length))
  val value = intarrayToBytearray(sm.colPtrs) ++ intarrayToBytearray(sm.rowIndices) ++ doublearrayToBytearray(sm.values)

  (key, meta ++ value)
}.saveAsSequenceFile(outpath)

// NOTE: below causes slow read performance when experiment
// bmat.blocks.saveAsObjectFile(outpath)

sys.exit

