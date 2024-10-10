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
                .setMaster("local")
                .setAppName("MLLibMatrixOps")
    val sc = new SparkContext(conf)
    val ml = new MLContext(sc)
    val spark = SparkSession.builder.getOrCreate()
    
    // set systemds configuration file path and print statistics
    val sysds = sys.env("SYSTEMDS_ROOT")
    ml.setConfig(s"${sysds}/conf/SystemDS-config.xml")
    ml.setStatistics(true)

    val argMap = Map[String,String](
            args.map(_.split("=")).map({
                case Array(x, y) => (x -> y)
            }):_*
        )

    val opType = argMap("opType")
    val nrow = argMap("nrow")
    val iter = argMap("iter").toInt

    val inputX_LR = argMap("inputX_LR")
    val inputY = argMap("inputY")
    val inputW_LR = argMap("inputW_LR")

    val inputX_NMF = argMap("inputX_NMF")
    val inputW_NMF = argMap("inputW_NMF")
    val inputH = argMap("inputH")

    val outputB = argMap("outputB") // output of LR
    val outputW = argMap("outputW") // output of NMF
    val outputH = argMap("outputH") // output of NMF
    
    val call = opType match {
        case "lr" => s"lr(${nrow})"
        case "nmf" => s"nmf(${nrow})"
        case _        => ""
    }

    // set block size
    val bs = nrow.toInt / 100

    val dmlText =
    s"""
        | tmp = ${call}
        |
        | lr = function(Integer nrow) return (integer res) {
        |
        |   X = read("$inputX_LR")
        |   y = read("$inputY")
        |   w = read("$inputW_LR")
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
        |   write (w, "$outputB", format="binary")
        |
        |   res = iteration
        | }
        |
        | nmf = function(Integer nrow) return (integer res) {
        |
        |   X = read ("$inputX_NMF")
        |   W = read ("$inputW_NMF")
        |   H = read ("$inputH")
        |
        |   iteration = 0
        |   
        |   while (iteration < $iter) {
        |     W = W * ((X %*% t(H)) / (W %*% (H %*% t(H))))
        |     H = H * ((t(W) %*% X) / ((t(W) %*% W) %*% H))
        |
        |     iteration = iteration + 1
        |   }
        |
        |   write (W, "$outputW", format="binary", rows_in_block=$bs)
        |   write (H, "$outputH", format="binary")
        |
        |   res = iteration
        | }
    """.stripMargin
    val script = dml(dmlText)
    val res = ml.execute(script)
}
