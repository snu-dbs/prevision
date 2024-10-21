import scala.math._
import java.nio.file.{Paths, Files, StandardOpenOption}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import breeze.{math => bMath,
               numerics => bNum,
               linalg => bAlg}
import breeze.linalg.{Vector => bVector,
                      Matrix => bMatrix,
                      SparseVector => bSparseVector,
                      DenseVector => bDenseVector,
                      CSCMatrix => bSparseMatrix,
                      DenseMatrix => bDenseMatrix}
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg
import org.apache.spark.ml.{linalg => alg}
import org.apache.spark.mllib.random.RandomRDDs._
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.sql._
import scala.io.Source
import java.util.Random
// import scala.collection.immutable._
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices, Matrix}
import java.nio.ByteBuffer
import org.apache.spark.sql.types._


object SparkMLAlgorithms {
    def main(args: Array[String]): Unit= {
        val conf = new SparkConf()
        .setAppName("MLLibMatrixOps")
       
        val sc = new SparkContext(conf)

        val argMap = Map[String,String](
                args.map(_.split("=")).map({
                    case Array(x,y) => (x -> y)
                }):_*
            )

        val opType = argMap("opType")
        val mattype = argMap("mattype")
        val dataPath = argMap.get("dataPath")       
        val nrow = argMap("nrow").toInt
        val density = argMap("density")
        val dataset = argMap("dataset")
        val noi = argMap("noi").toInt

        /* Start */
        val start = System.nanoTime()

        if (opType == "logit")
            logit(nrow, noi, sc)
        else if (opType == "gnmf")
            gnmf(nrow, 10, noi, sc)
        else if (opType == "slogit")
            logit_sparse(density, noi, sc)
        else if (opType == "pagerank")
            pagerank(dataset, nrow, (nrow + 9) / 10, noi, sc)
        else if (opType == "pagerank2")
            pagerank(dataset, nrow, (nrow + 19) / 20, noi, sc)

        /* Stop */
        val stop = System.nanoTime()
        println("Elapsed Time (s): " + (stop - start)/1e9)

        // scala.io.StdIn.readLine()
    }

    def logit(nrow: Int, max_iter: Int = 3, sc: SparkContext) = {
        // input X
        val xraw = sc.sequenceFile[String, Array[Byte]](s"../../slab-benchmark/prevision/output/sequencefile/${nrow}x100_dense.sf")
        val blocksX = xraw.map{ x =>
            val items = x._1.split(",").map(_.toInt)
            ((items(0), items(1)), Matrices.dense(nrow/100, 100, bytearrayToDoublearray(x._2)))
        }
        val X = new BlockMatrix(blocksX, nrow/100, 100)
        X.blocks.persist(MEMORY_AND_DISK_SER)

        var iteration = 0
        var step_size = 0.0000001
        val N = X.numRows.toInt
        val K = X.numCols.toInt

        val yraw = sc.sequenceFile[String, Array[Byte]](s"../../slab-benchmark/prevision/output/sequencefile/${N}x1_dense.sf")
        val blocksY = yraw.map{ x =>
            val items = x._1.split(",").map(_.toInt)
            ((items(0), items(1)), Matrices.dense(N/100, 1, bytearrayToDoublearray(x._2)))
        }
        val y = new BlockMatrix(blocksY, N/100, 1)
        y.persist(MEMORY_AND_DISK_SER)

        val wraw = sc.sequenceFile[String, Array[Byte]]("../../slab-benchmark/prevision/output/sequencefile/100x1_dense.sf")
        var w = wraw.map( x => Vectors.dense(bytearrayToDoublearray(x._2))).first().asInstanceOf[DenseVector]

        val XT = X.transpose
        XT.persist(MEMORY_AND_DISK_SER)

        while (iteration < max_iter) {
            // println(s"Iteration => ${iteration}")
	    val xbBlocks = X.blocks.map(x => (x._1, x._2.multiply( w )))
            // xbBlocks.persist(MEMORY_AND_DISK_SER)

	    val ggBlocks = xbBlocks.map { b => 
	        val cal = b._2.values.map(x => 1 / (1 + exp(-x)))
	        (b._1, cal)
            }
            // ggBlocks.persist(MEMORY_AND_DISK_SER)

	    val epsBlocks = ggBlocks.join(y.blocks).map { x =>
	        val rid = x._1._1
                val cid = x._1._2
                val blocks = x._2
                val left = bDenseVector[Double](blocks._1)
                val right = bDenseVector[Double](blocks._2.asInstanceOf[DenseMatrix].values)
                val res = left -:- right
	        ((cid, rid), res)        // reversed due to joining with XT.blocks
	    }
            // epsBlocks.persist(MEMORY_AND_DISK_SER)

            // manually perform Matrix-Vector multiplication since blockmatrix does not support this mv mult.
	    val XTeBlocks = XT.blocks.join(epsBlocks).map { case ((rid, cid), blocks) =>
	        val mat = blocks._1
	        val vec = from_breeze(blocks._2)
	        val res = mat.multiply(vec)
	        res
	    }
            // XTeBlocks.persist(MEMORY_AND_DISK_SER)

	    val XTeVec = XTeBlocks.reduce{(x, y) => 
	        val left = as_breeze(x)
	        val right = as_breeze(y)
	        val res = left +:+ right
	    
	        from_breeze(res).asInstanceOf[DenseVector]
	    }

	    val w_update = (step_size) * as_breeze(XTeVec)
	    w = from_breeze( as_breeze(w) - w_update).asInstanceOf[DenseVector]

            iteration += 1
        }

        sc.parallelize(Seq(("0,0", doublearrayToBytearray(w.values)))).saveAsSequenceFile("__mllib_logit.sf")
    }

    def read_blockmatrix_sparse(path: String, numRow: Int, numCol: Int, numBlockRow: Int, numBlockCol: Int, sc: SparkContext) : BlockMatrix = {
        val raw = sc.sequenceFile[String, Array[Byte]](path)
        val blocks = raw.map{ x =>
          val items = x._1.split(",").map(_.toInt)
          val key = (items(0), items(1))

          var base = 4 * 3    // sizeof(int) * 3 
          val metabytes = x._2.slice(0, base)      
          val meta = bytearrayToIntarray(metabytes)

          val colPtrs = bytearrayToIntarray(x._2.slice(base, base + (meta(0) * 4)))
          base = base + (meta(0) * 4)

          val rowIndices = bytearrayToIntarray(x._2.slice(base, base + (meta(1) * 4)))
          base = base + (meta(1) * 4)

          val values = bytearrayToDoublearray(x._2.slice(base, base + (meta(2) * 8)))

          val value = Matrices.sparse(numBlockRow, numBlockCol, colPtrs, rowIndices, values)
          (key, value)
        }
        return new BlockMatrix(blocks, numBlockRow, numBlockCol)
    }

    def logit_sparse(density: String, max_iter: Int = 3, sc: SparkContext) = {
        // input X
        var X = read_blockmatrix_sparse(
          s"../../slab-benchmark/prevision/output/sequencefile/400000000x100_sparse_${density}.sf",
          400000000, 100, 4000000, 100, sc)
        X.blocks.persist(MEMORY_AND_DISK_SER)

        var iteration = 0
        var step_size = 0.0000001
        val N = X.numRows.toInt
        val K = X.numCols.toInt

        // FIXME:
        var y = read_blockmatrix_sparse(
          s"../../slab-benchmark/prevision/output/sequencefile/400000000x1_sparse_${density}.sf",
          400000000, 1, 4000000, 1, sc)
        y.persist(MEMORY_AND_DISK_SER)

        // w is sparse when loaded, but it will become densevector as computed.
        val wraw = sc.sequenceFile[String, Array[Byte]](s"../../slab-benchmark/prevision/output/sequencefile/100x1_sparse_${density}.sf")
        var w = wraw.map { x => 
          var base = 4 * 3    // sizeof(int) * 3 
          val metabytes = x._2.slice(0, base)      
          val meta = bytearrayToIntarray(metabytes)
          base = base + (meta(0) * 4)

          val rowIndices = bytearrayToIntarray(x._2.slice(base, base + (meta(1) * 4)))
          base = base + (meta(1) * 4)

          val values = bytearrayToDoublearray(x._2.slice(base, base + (meta(2) * 8)))

          Vectors.sparse(100, rowIndices, values)
        }.first().asInstanceOf[Vector]

        val XT = X.transpose
        XT.persist(MEMORY_AND_DISK_SER)

        while (iteration < max_iter) {
            // println(s"Iteration => ${iteration}")
	    val xbBlocks = X.blocks.map{ x => 
              if (iteration == 0) (x._1, x._2.multiply( w.asInstanceOf[SparseVector] ))
              else (x._1, x._2.multiply( w.asInstanceOf[DenseVector] ))
            }
            // xbBlocks.persist(MEMORY_AND_DISK_SER)

	    val ggBlocks = xbBlocks.map { b => 
	        val cal = b._2.values.map(x => 1 / (1 + exp(-x)))
	        (b._1, cal)
            }
            // ggBlocks.persist(MEMORY_AND_DISK_SER)

	    val epsBlocks = ggBlocks.join(y.blocks).map { x =>
	        val rid = x._1._1
                val cid = x._1._2
                val blocks = x._2
                val left = bDenseVector[Double](blocks._1)
                val rsmat = blocks._2.asInstanceOf[SparseMatrix]
                val right = new bSparseVector[Double](rsmat.rowIndices, rsmat.values, 4000000).toDenseVector
                val res = left -:- right
	        ((cid, rid), res)        // reversed due to joining with XT.blocks
	    }
            // epsBlocks.persist(MEMORY_AND_DISK_SER)

            // manually perform Matrix-Vector multiplication since blockmatrix does not support this mv mult.
	    val XTeBlocks = XT.blocks.join(epsBlocks).map { case ((rid, cid), blocks) =>
	        val mat = blocks._1
	        val vec = from_breeze(blocks._2)
	        val res = mat.multiply(vec)
	        res
	    }
            // XTeBlocks.persist(MEMORY_AND_DISK_SER)

	    val XTeVec = XTeBlocks.reduce{(x, y) => 
	        val left = as_breeze(x)
	        val right = as_breeze(y)
	        val res = left +:+ right
	    
	        from_breeze(res).asInstanceOf[DenseVector]
	    }

	    val w_update = (step_size) * as_breeze(XTeVec)
	    w = from_breeze( as_breeze(w) - w_update).asInstanceOf[Vector]

            iteration += 1
        }

        sc.parallelize(Seq(("0,0", doublearrayToBytearray(w.asInstanceOf[DenseVector].values)))).saveAsSequenceFile("__mllib_logit.sf")
    }

    def pagerank(dataset: String, arrside: Int, tileside: Int, max_iter: Int = 3, sc: SparkContext) = {
        var d = 0.85

        // input X
        var X = read_blockmatrix_sparse(
          s"../../slab-benchmark/prevision/output/sequencefile/${dataset}.sf",
          arrside, arrside, tileside, tileside, sc)
        X.blocks.persist(MEMORY_AND_DISK_SER)

        /*
        var v = read_blockmatrix_sparse(
          s"../../slab-benchmark/prevision/output/sequencefile/${dataset}_v.sf",
          arrside, 1, tileside, 1, sc)
        v.persist(MEMORY_AND_DISK_SER)
        */

        val vraw = sc.sequenceFile[String, Array[Byte]](s"../../slab-benchmark/prevision/output/sequencefile/${dataset}_v.sf")
        val blocksV = vraw.map{ x =>
            val items = x._1.split(",").map(_.toInt)
            ((items(0), items(1)), Matrices.dense(tileside, 1, bytearrayToDoublearray(x._2)))
        }
        var v = new BlockMatrix(blocksV, tileside, 1)
        v.blocks.persist(MEMORY_AND_DISK_SER)

        // var with_one = bDenseVector[Double](Array.fill[Double](arrside)((1.toDouble - d) / arrside) ++ Array.fill[Double](arrside - (arrside % tileside))(0))

        var iteration = 0
        while (iteration < max_iter) {
            var MR = X.multiply(v)

            var newBlocks = MR.blocks.map { x =>
                var vals = x._2.asInstanceOf[DenseMatrix].values
                for (i <- 0 until vals.length) {
                  if (tileside * x._1._1 + i < arrside)
                    vals(i) = d * vals(i) + ((1.toDouble - d) / arrside)
                  else
                    vals(i) = 0  // for padding. It might be faster than adding with_one
                }
              (x._1, Matrices.dense(tileside, 1, vals))
            }
            v = new BlockMatrix(newBlocks, tileside, 1)
            // v = from_breeze((d * as_breeze(MR)) + with_one)

            iteration += 1
        }

        v.blocks.map(x => ((x._1._1.toString + "," + x._1._2.toString),
	    doublearrayToBytearray(x._2.asInstanceOf[DenseMatrix].values))).saveAsSequenceFile("__pagerank_res.sf")
    }

    def gnmf(nrow: Int, r: Int, max_iter: Int, sc: SparkContext) = {
        // input X
        val xraw = sc.sequenceFile[String, Array[Byte]](s"../../slab-benchmark/prevision/output/sequencefile/${nrow}x100_dense.sf")
        val blocksX = xraw.map{ x =>
            val items = x._1.split(",").map(_.toInt)
            ((items(0), items(1)), Matrices.dense(nrow/100, 100, bytearrayToDoublearray(x._2)))
        }
        val X = new BlockMatrix(blocksX, nrow/100, 100)
        X.blocks.persist(MEMORY_AND_DISK_SER)

        val N = X.numRows.toInt
        val K = X.numCols.toInt
        // row x rank
        // rank x column
       
        val w = sc.sequenceFile[String, Array[Byte]](s"../../slab-benchmark/prevision/output/sequencefile/${N}x10_dense.sf")
        val blocksW = w.map{ x =>
            val items = x._1.split(",").map(_.toInt)
            ((items(0), items(1)), Matrices.dense(N/100, 10, bytearrayToDoublearray(x._2)))
        }
        var W = new BlockMatrix(blocksW, N/100, 10)
        W.blocks.persist(MEMORY_AND_DISK_SER)

        val h = sc.sequenceFile[String, Array[Byte]]("../../slab-benchmark/prevision/output/sequencefile/10x100_dense.sf")
        val blocksH = h.map{ x =>
            val items = x._1.split(",").map(_.toInt)
            ((items(0), items(1)), Matrices.dense(10, 100, bytearrayToDoublearray(x._2)))
        }
        var H = new BlockMatrix(blocksH, 10, 100)
        H.blocks.persist(MEMORY_AND_DISK_SER)

        var iteration = 0
        while (iteration < max_iter) {
            // println(s"Iteration => ${iteration}")
            W = elem_multiply(W, elem_divide( X.multiply( H.transpose, 100 ), W.multiply( H.multiply( H.transpose, 100 ), 100)))
            H = elem_multiply(H, elem_divide( W.transpose.multiply( X, 100 ),
                                (W.transpose.multiply( W, 100 ).multiply( H, 100 ))))
            iteration = iteration + 1

            W.blocks.persist(MEMORY_AND_DISK_SER)
            H.blocks.persist(MEMORY_AND_DISK_SER)
        }

        W.blocks.map(x => ((x._1._1.toString + "," + x._1._2.toString),
	    doublearrayToBytearray(x._2.asInstanceOf[DenseMatrix].values))).saveAsSequenceFile("__mllib_gnmf_W.sf")
        H.blocks.map(x => ((x._1._1.toString + "," + x._1._2.toString),
	    doublearrayToBytearray(x._2.asInstanceOf[DenseMatrix].values))).saveAsSequenceFile("__mllib_gnmf_H.sf")
    }

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

    def vectorize(M: IndexedRowMatrix) : Matrix = {
        val loc = M.toCoordinateMatrix.toBlockMatrix()
        return loc.toLocalMatrix()
    }

    // Spark annoyingly does not expose any of these primitive methods
    // which makes their library not very useful
    def as_breeze(v: linalg.Vector) : bVector[Double] = v match {
        case v: linalg.SparseVector =>
            return new bSparseVector[Double](v.indices, v.values, v.size)
        case v: linalg.DenseVector  =>
            return new bDenseVector[Double](v.values)
    }

    def from_breeze(v: bVector[Double]) : linalg.Vector = v match {
        case v: bSparseVector[Double] =>
            return Vectors.sparse(v.length, v.activeIterator.toSeq)
        case v: bDenseVector[Double] =>
            return Vectors.dense(v.data)
    }

    def as_breeze(m: linalg.Matrix) : bMatrix[Double] = m match {
        case m: linalg.DenseMatrix =>
            return new bDenseMatrix(m.numRows, m.numCols, m.asInstanceOf[DenseMatrix].values)
        case m: linalg.SparseMatrix =>
            return new bSparseMatrix(
                m.values, m.numRows, m.numCols,
                m.colPtrs, m.numActives, m.rowIndices)
    }

    def from_breeze(m: bMatrix[Double]) : linalg.Matrix = m match {
        case m: bDenseMatrix[Double] =>
            return Matrices.dense(m.rows, m.cols, m.toDenseMatrix.data)
        case m: bSparseMatrix[Double] =>
            return Matrices.sparse(m.rows, m.cols,
                    m.colPtrs, m.rowIndices, m.data)
    }

    def elem_subtract(A: BlockMatrix, B: BlockMatrix) : BlockMatrix = {
        val both = join_block_matrices( A, B )
        val new_blocks = both.map(block => ((block._1._1, block._1._2),
            from_breeze( as_breeze( block._2._1 ) -:- as_breeze( block._2._2 ))))

        return new BlockMatrix(new_blocks, A.rowsPerBlock, A.colsPerBlock)
    }

    def transpose_row_matrix(A: IndexedRowMatrix) : IndexedRowMatrix = {
        // hacky way to transpose an IRM
        return A.toCoordinateMatrix.transpose.toIndexedRowMatrix
    }

    def join_row_matrices(A: IndexedRowMatrix,
                          B: IndexedRowMatrix) :
            RDD[(Long, (linalg.Vector,linalg.Vector))] = {
        val pair_A = A.rows.map(row => (row.index, row.vector))
        val pair_B = B.rows.map(row => (row.index, row.vector))
        return pair_A.join(pair_B)
    }

    def scalar_multiply(v: Double, M: IndexedRowMatrix) : IndexedRowMatrix = {
        val rows_rdd = M.rows.map(row => new IndexedRow(row.index,
                from_breeze(v*as_breeze(row.vector))))
        return new IndexedRowMatrix(rows_rdd)
    }

    def reg(X: IndexedRowMatrix, y: IndexedRowMatrix) : Matrix = {
        val XTX = X.computeGramianMatrix()
        val XTY = X.toBlockMatrix(1024,X.numCols.toInt).transpose.
                    multiply( y.toBlockMatrix(1024,1), 500 ).
                    toLocalMatrix
        val b = from_breeze( to_dense( as_breeze( XTX ) ) \
                             to_dense( as_breeze( XTY ) ) )
        return b
    }

    def to_dense(M: bMatrix[Double]) : bDenseMatrix[Double] = {
        return M.asInstanceOf[bDenseMatrix[Double]]
    }

    def random_matrix(N: Int, K: Int,
                      r: Int, c: Int, sc: SparkContext) : BlockMatrix = {
        val MM = new IndexedRowMatrix(
            normalVectorRDD(sc, N.toLong, K).zipWithIndex().map(
                tup => new IndexedRow(tup._2, tup._1))).toBlockMatrix(r, c)
        return MM
    }

    def join_block_matrices(A: BlockMatrix,
                            B: BlockMatrix) :
            RDD[((Int, Int), (linalg.Matrix,linalg.Matrix))] = {
        val pair_A = A.blocks.map(block => block)
        val pair_B = B.blocks.map(block => block)
        return pair_A.join(pair_B)
    }

    def elem_divide(A: BlockMatrix, B: BlockMatrix) : BlockMatrix = {
    val both = join_block_matrices( A, B )
    val new_blocks = both.map(block => ((block._1._1, block._1._2),
        from_breeze( as_breeze( block._2._1 ) /:/ as_breeze( block._2._2 ))))
    return new BlockMatrix(new_blocks, A.rowsPerBlock, A.colsPerBlock)
    }

    def elem_multiply(A: BlockMatrix, B: BlockMatrix) : BlockMatrix = {
        val both = join_block_matrices( A, B )
        val new_blocks = both.map(block => (block._1,
            from_breeze( as_breeze( block._2._1 ) *:* as_breeze( block._2._2 ))))
        return new BlockMatrix(new_blocks, A.rowsPerBlock, A.colsPerBlock)
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


}

