import scala.math._
import java.nio.file.{Paths, Files}
import org.apache.sysds.api.mlcontext._
import org.apache.sysds.api.mlcontext.ScriptFactory._
import org.apache.sysds.api.mlcontext.MatrixFormat._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import org.apache.spark.sql._
import scala.collection.immutable._
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.ml.{linalg => alg}
import org.apache.spark.mllib.{linalg => malg}
import org.apache.spark.mllib.linalg.distributed._

object SystemDSMLAlgorithms extends App {
    val conf = new SparkConf()
                .setAppName("MLLibMatrixOps")
    val sc = new SparkContext(conf)
    val ml = new MLContext(sc)
    val spark = SparkSession.builder.getOrCreate()

    // set systemds configuration file path and print statistics
    val sysds = sys.env("SYSTEMDS_ROOT")
    ml.setConfig(s"${sysds}/conf/SystemDS-config.xml")
    ml.setConfigProperty("sysds.native.blas", "openblas")
    ml.setStatistics(true)

    val argMap = Map[String,String](
            args.map(_.split("=")).map({
                case Array(x, y) => (x -> y)
            }):_*
        )

    val opType = argMap("opType")
    val nrow = argMap("nrow")
    val iter = argMap("iter").toInt

    val inputX = argMap("inputX")
    val inputY = argMap("inputY")
    val inputW = argMap("inputW")
    val inputM = argMap("inputM")

    val output = argMap("output") // output of LR

    val call = opType match {
        case "lr" => s"lr(${nrow})"
        case "pr" => s"pr(${nrow})"
        case _        => ""
    }

    // set block size
    val bs = (nrow.toInt + 9) / 10
    
    val dmlText =
    s"""
        | tmp = ${call}
        |
        | lr = function(Integer nrow) return (integer res) {
        |
        |   X = read("$inputX")
        |   y = read("$inputY")
        |   w = read("$inputW")
        |
        |   iteration = 0
        |   stepSize = 0.0000001
        |
        |   while (iteration < $iter) {
        |       xb = X %*% w
        |       delta = 1 / (1 + exp (-xb)) - y
        |
        |       w = w - ((stepSize * t(X) %*% delta))
        |    
        |       iteration = iteration + 1
        |   }
        |
        |   write (w, "$output", format="binary")
        |
        |   res = iteration
        | }
        |
        | pr = function(Integer nrow) return (integer res) {
        |
        |   M = read ("$inputM")
        |
        |   iteration = 0
        |
        |   N = ncol(M)
        |   v = matrix(1.0, rows=N, cols=1) / N
        |   d = 0.85
        |
        |   while (iteration < $iter) {
        |     v = d * M %*% v + (1 - d) / N
        |     
        |     iteration = iteration + 1
        |   }
        |   
        |   write (v, "$output", format="binary", rows_in_block=$bs)
        |
        |   res = iteration
        | }
    """.stripMargin
    val script = dml(dmlText)
    val res = ml.execute(script)
}
