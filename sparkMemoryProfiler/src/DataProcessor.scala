import scala.io.Source
import scala.collection.mutable.MutableList
import java.io._
import scala.util.control.Exception.allCatch
import scala.collection.immutable.ListMap

/**
 * @author yuzhanghan1982
 */
object DataProcessor {

  /*
  def main(args: Array[String]) {

    var fileMap: List[(String, Map[String, Long])] = List()

    new File(args(0))
      //    new File("/home/yuzhanghan1982/2015summer/results/test1/")
      .listFiles.toIterator.toSeq.sortBy { f => f.getName() }
      .foreach {
        f =>
//          println(f.getName())
          var classSize: Map[String, Long] = Map()
          Source.fromFile(f).getLines().filter(l => !(l.contains("#") | l.contains("--") | l.contains("Total") | l.size == 0)).foreach {
            l =>
              //              println(l)
              var tokens: Array[String] = l.split("\\s+").toArray
              classSize += (tokens(3) -> tokens(2).toLong)
          }
          fileMap = fileMap :+ (f.getName, classSize)
      }

    fileMap.foreach {
      fm =>
        println(fm._1 + " -> " + fm._2.size)
    }

  }
  */
  def main(args: Array[String]) {

    if (args.length > 2 || args.length < 1) {
      println("Usage:\nscala DataProcessor <input directory> [input data size (default: 1)]");
      return
    }

    var dataSize: Long = 1
    if (args.length == 2)
      dataSize = args(1).toLong
    var class_totalSize: Map[String, Long] = Map()

    new File(args(0))
      .listFiles.filter(f => f.getName().startsWith("sparkOutput_worker_application_")).toIterator.toSeq.sortBy { f => f.getName() }
      .foreach {
        f =>
          //                    println(f.getName())
          Source.fromFile(f).getLines().filter(l => !(l.contains("#") | l.contains("--") | l.contains("Total") | l.size == 0)).foreach {
            l =>
              //                            println(l)
              var tokens: Array[String] = l.split("\\s+").toArray
              //              println(tokens.last + " -> " + tokens(tokens.length-2))
              if (class_totalSize.get(tokens.last) != None) {
                val value: Long = class_totalSize.get(tokens.last).get + tokens(tokens.length - 2).toLong
                class_totalSize += (tokens.last -> value)
              } else
                class_totalSize += (tokens.last -> tokens(tokens.length - 2).toLong)

          }
      }

    var result: Map[String, Long] = Map()

    val filename = new File(args(0)).listFiles.apply(0).getAbsolutePath()
    val filename_output = filename.substring(0, filename.lastIndexOf('_')).replace("sparkOutput_worker", "instanceAggregatedSize") + ".txt"
    println("Output file: " + filename_output)
    var f: File = new File(filename_output)
    if (f.exists())
      f.delete()
    val writer = new FileWriter(f, true)

    class_totalSize.toSeq.foreach {
      ct =>
        result += (ct._1 -> ct._2 / dataSize)
    }

    result.toSeq.sortBy(ct => ct._2)(Ordering[Long].reverse).foreach {
      ct =>
        //        println(ct._1 + " -> " + ct._2)
        writer.write(ct._1 + "\t" + ct._2 + "\n")
    }
    writer.flush()
    writer.close()
    //    println(class_totalSize)

  }
}