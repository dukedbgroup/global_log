import scala.io.Source
import scala.collection.mutable.MutableList
import java.io._
import scala.util.control.Exception.allCatch
import scala.collection.immutable.ListMap

/**
 * @author yuzhanghan1982
 */
object ApplicationStatistic {

  def main(args: Array[String]) {

    var applicationID: String = "application_1447540585201_" + args(0)
//    var applicationID: String = "application_1446655589988_" + args(0)
    var testName: String = "test63"
    
    var filename: String = "/home/mayuresh/heap-logs/" + applicationID

    var executor_maxUsedHeap: Map[Int, Long] = Map()
    var executor_medUsedHeap: Map[Int, Long] = Map()
    var executor_minUsedHeap: Map[Int, Long] = Map()
    var executor_time: Map[Int, Long] = Map()

    println(applicationID)
    new File(filename).listFiles.filter(f => f.isDirectory()).foreach {
      d =>
//        println(d.getName())
        var f: File = d.listFiles().filter { f => f.getName().startsWith("sparkOutput_worker_application_") }(0)
        var values_usedHeap: MutableList[Long] = new MutableList()
//        println(f.getName())
//        println(f)
        Source.fromFile(f).getLines().filter(l => !(l.contains("application") | l.contains("Code") | l.size == 0)).foreach {

          l =>
            //              println(l)
            values_usedHeap += l.split("\\t")(5).toLong
        }
        values_usedHeap = values_usedHeap.sorted
        executor_minUsedHeap += ((d.getName().toInt, values_usedHeap.head))
        executor_medUsedHeap += ((d.getName().toInt, values_usedHeap(values_usedHeap.length / 2)))
        executor_maxUsedHeap += ((d.getName().toInt, values_usedHeap.last))
        executor_time += ((d.getName().toInt, values_usedHeap.size))
    }

    println("Application result:\nExecutor - max used heap")
    var result = executor_maxUsedHeap.toList.sortBy(_._2).last
    println(result._1 + "\t" + result._2 / 1000000 + " MB")

/*    println("\n\nExecutor - max used heap")
    executor_maxUsedHeap.toList.sortBy(_._1).foreach {
      e =>
        println(e._1 + "\t" + e._2 / 1000000 + " MB")
    }
    println("Executor - med used heap")
    executor_medUsedHeap.toList.sortBy(_._1).foreach {
      e =>
        println(e._1 + "\t" + e._2 / 1000000 + " MB")
    }

    println("Executor - min used heap")
    executor_minUsedHeap.toList.sortBy(_._1).foreach {
      e =>
        println(e._1 + "\t" + e._2 / 1000000 + " MB")
    }
    println("Executor - time")
    executor_time.toList.sortBy(_._1).foreach {
      e =>
        println(e._1 + "\t" + e._2)
    }*/
  }
}
