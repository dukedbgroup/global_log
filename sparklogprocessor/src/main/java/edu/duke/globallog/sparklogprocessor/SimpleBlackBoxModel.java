package edu.duke.globallog.sparklogprocessor;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.stream.*;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

public class SimpleBlackBoxModel extends TrainingBase {

  String APP_ID;

    SummaryStatistics beta0 = new SummaryStatistics();
    SummaryStatistics beta1 = new SummaryStatistics();
    SummaryStatistics gcMem = new SummaryStatistics();
    SummaryStatistics maxStorage = new SummaryStatistics();
    SummaryStatistics maxExecution = new SummaryStatistics();
    SummaryStatistics maxUnmgd = new SummaryStatistics();
    SummaryStatistics maxHeap = new SummaryStatistics();
    SummaryStatistics avgCPU = new SummaryStatistics();

  Double cacheHitRatio, execSpillageFraction;

  // config
  Double heapSize, sparkMemoryFraction;
  Integer numExecs, newRatio, maxCores;
  Double survivorRegion, oldSize, youngSize;
  Double DELTA = 0.1;

  public SimpleBlackBoxModel(String appId) {
    extractConfig(appId);
    startConnection();
  }

  void cleanup() {
    commit();
    stopConnection();
  }

  private void extractConfig(String appId) {
    APP_ID = appId;
    // hardcoding now, should be extracted from db
    heapSize = 4404*1024*1024d;
    maxCores = 2;
    sparkMemoryFraction = 0.6;
    newRatio = 2;
    numExecs = 1; // per node
    resetGCPools();
  }

  private void resetGCPools() {
    int survivorRatio = 8; // hardcoding
    survivorRegion = heapSize / (newRatio + 1) / survivorRatio;
    youngSize = heapSize / (newRatio + 1) - survivorRegion;
    oldSize = heapSize - youngSize - survivorRegion;
  }

  private void learnFromDB() throws Exception {
    cacheHitRatio = 1d;
    execSpillageFraction = 0d;
    // query for KMeans
    String qsql1 = "select count(1) as cnt" +
             " from " + TASK_METRICS_TABLE + " where appId=\"" + APP_ID + "\" " +
             "and taskId in (select taskId from " + TASK_METRICS_TABLE + 
             " where appId=\"" + APP_ID + "\" and stageId >= 3 and " +
             "stageId <= 13 and locality = \'process_local\' " +
             "group by taskId having count(1)=1)";

    // query for SortByKey
/*    String qsql1 = "select count(1) as cnt" +
             " from " + TASK_METRICS_TABLE + " where appId=\"" + APP_ID + "\" " +
             "and taskId in (select taskId from " + TASK_METRICS_TABLE +
             " where appId=\"" + APP_ID + "\" and stageId = 3 " +
             "group by taskId having count(1)=1)";
*/    // query for KMeans
    String qsql2 = "select count(1) as cnt, sum(shLocalBytesRead+shRemoteBytesRead) as shRead, " +
             "sum(diskSpilled) as spilled " +
             " from " + TASK_METRICS_TABLE + " where appId=\"" + APP_ID + "\" " +
             "and taskId in (select taskId from " + TASK_METRICS_TABLE + 
             " where appId=\"" + APP_ID + "\" and stageId >= 3 and stageId <= 13 " +
             "group by taskId having count(1)=1)";

    // query for SortByKey
/*    String qsql2 = "select count(1) as cnt, sum(shLocalBytesRead+shRemoteBytesRead) as shRead, " +
             "sum(diskSpilled) as spilled " +
             " from " + TASK_METRICS_TABLE + " where appId=\"" + APP_ID + "\" " +
             "and taskId in (select taskId from " + TASK_METRICS_TABLE +
             " where appId=\"" + APP_ID + "\" and stageId = 3 " +
             "group by taskId having count(1)=1)";
*/
    PreparedStatement qstmt1 = newPreparedStatement(qsql1);
    PreparedStatement qstmt2 = newPreparedStatement(qsql2);
    ResultSet rs1 = qstmt1.executeQuery();
    ResultSet rs2 = qstmt2.executeQuery();
    double pLocal = 0d;
    if(rs1.next()) {
      pLocal = rs1.getDouble("cnt");
    }
    if(rs2.next()) {
      double total = rs2.getDouble("cnt");
      cacheHitRatio = pLocal / total;
      double shTotal = rs2.getDouble("shRead");
      double shSpilled = rs2.getDouble("spilled");
      execSpillageFraction = shSpilled / shTotal;
    }
System.out.println("Cache hit ratio: " + cacheHitRatio);
System.out.println("Exec Spillage Fraction: " + execSpillageFraction);
  }

  private void learnFromLogs() throws Exception {
    int numExamples = 7; // HACK: hardcoding

    for(int execId=1; execId<=numExamples; execId++) {
        // hard-coding file path pattern
        String fileToRead = PERF_MONITOR_HOME + APP_ID + "/" + execId + "/" + PERF_FILE_PREFIX + APP_ID + "_" + execId + ".txt";
//System.out.println("Reading file: " + fileToRead);
        Path path = Paths.get(fileToRead);
        Stream<String> lines = Files.lines(path);
        Long totalLines = lines.count() - 2; //first two header lines

        SummaryStatistics initHeap = new SummaryStatistics();
        SummaryStatistics taskHeap = new SummaryStatistics();
        SummaryStatistics heap = new SummaryStatistics();
        SummaryStatistics GCHeap = new SummaryStatistics();
        SummaryStatistics GCUnmgd = new SummaryStatistics();
        SummaryStatistics storage = new SummaryStatistics();
        SummaryStatistics execution = new SummaryStatistics();
        SummaryStatistics cpu = new SummaryStatistics();

        boolean noFullGC = true;
        lines = Files.lines(path);
        lines = lines.skip(2); // header lines
        int cnt = 0;
        double prevOld = 0d; // for old GC detection
        Iterator<String> iterator = lines.iterator();
        while(iterator.hasNext()) {
          String line = iterator.next();
          String[] tokens = line.split("\t");
          if(cnt < 10) // HACK: first 50 are assumed to be for initializing the executor
          {
            initHeap.addValue(Double.parseDouble(tokens[6]));
          }
          heap.addValue(Double.parseDouble(tokens[6]));
          cpu.addValue(Double.parseDouble(tokens[13]));
          storage.addValue(Double.parseDouble(tokens[20]));
          execution.addValue(Double.parseDouble(tokens[21]));
          if(Double.parseDouble(tokens[5]) < prevOld) { // old GC
            noFullGC = false;
            GCHeap.addValue(Double.parseDouble(tokens[6]));
//System.out.println("storage: " + tokens[20] + ", exec: " + tokens[21] + ", heap: " + tokens[6]);
            taskHeap.addValue((Double.parseDouble(tokens[6]) - Double.parseDouble(tokens[20]) - initHeap.getMax()) / maxCores);
            GCUnmgd.addValue((Double.parseDouble(tokens[6]) - Double.parseDouble(tokens[20]) - Double.parseDouble(tokens[21]) - initHeap.getMax()) / maxCores);
//            storage.addValue(Double.parseDouble(tokens[20]));
//            execution.addValue(Double.parseDouble(tokens[21]));
          }
          prevOld = Double.parseDouble(tokens[5]);
          cnt++;
        }

        if(noFullGC) { // not a single GC event found
System.out.println("No full GC found, using old gen size instead!");
          lines = Files.lines(path);
          lines = lines.skip(2); // header lines
          iterator = lines.iterator();
          while(iterator.hasNext()) {
            String line = iterator.next();
            String[] tokens = line.split("\t");
            GCHeap.addValue(Double.parseDouble(tokens[5]));
            taskHeap.addValue((Double.parseDouble(tokens[6]) - storage.getMax() - initHeap.getMax()) / maxCores);
            GCUnmgd.addValue((Double.parseDouble(tokens[6]) - storage.getMax() - Double.parseDouble(tokens[21]) - initHeap.getMax()) / maxCores);
//            storage.addValue(Double.parseDouble(tokens[20]));
//            execution.addValue(Double.parseDouble(tokens[21]));
          }

        }

if(!noFullGC) {
        beta0.addValue(initHeap.getMax());
        beta1.addValue(taskHeap.getMax());
        gcMem.addValue(GCHeap.getMax());
        maxUnmgd.addValue(GCUnmgd.getMax());
        maxHeap.addValue(heap.getMax());
        avgCPU.addValue(cpu.getMean());
        maxStorage.addValue(storage.getMax());
        maxExecution.addValue(execution.getMax());
}
    }
System.out.println("Max Heap: " + maxHeap.getMean() + "+-" + maxHeap.getStandardDeviation());
System.out.println("Max Storage: " + maxStorage.getMean() + "+-" + maxStorage.getStandardDeviation());
System.out.println("Max Execution: " + maxExecution.getMean() + "+-" + maxExecution.getStandardDeviation());
System.out.println("Avg CPU: " + avgCPU.getMean() + "+-" + avgCPU.getStandardDeviation());
System.out.println("Beta 0: " + beta0.getMean() + "+-" + beta0.getStandardDeviation() + ", max: " + beta0.getMax());
System.out.println("Beta 1: " + beta1.getMean() + "+-" + beta1.getStandardDeviation() + ", max: " + beta1.getMax());
System.out.println("GC Heap: " + gcMem.getMean() + "+-" + gcMem.getStandardDeviation() + ", max: " + gcMem.getMax());
System.out.println("Max Unmgd: " + maxUnmgd.getMean() + "+-" + maxUnmgd.getStandardDeviation() + ", max: " + maxUnmgd.getMax());
  }

  public void learn() {
    try {
      startConnection();
//      learnFromDB();
cacheHitRatio = 0.99d;
execSpillageFraction = 0d;
      learnFromLogs();
    } catch(Exception e) {
      e.printStackTrace();
    } finally {
      stopConnection();
    }
  }

  public void makeReliable() {
    if(beta0.getMax() + beta1.getMax() > (1-DELTA) * heapSize) {
System.out.println("Insufficient heap size: " + heapSize);
      return;
    }

/*    if(beta0.getMax() + maxCores * beta1.getMax() > (1-DELTA) * heapSize) {
System.out.println("DoP is too high: " + maxCores);
      maxCores = (int) (((1-DELTA) * heapSize - beta0.getMax()) / beta1.getMax());
System.out.println("*New recommendation: DoP = " + maxCores);
    }
*/
    int cnt=0;
    while(beta0.getMax() + maxCores * beta1.getMax() + sparkMemoryFraction * (heapSize-survivorRegion) > oldSize) {
System.out.println("Probing for: " + maxCores + ", " + sparkMemoryFraction + ", " + newRatio);
      if(cnt % 3 == 0) { 
        if(maxCores > 1) {
          maxCores--;
        } else {
          cnt++;
        }
      } 
      if(cnt % 3 == 1) {
        if(sparkMemoryFraction - (beta1.getMax() / (heapSize - survivorRegion)) > 0) {
          sparkMemoryFraction -= beta1.getMax() / (heapSize - survivorRegion);
          newRatio = (int) Math.ceil(sparkMemoryFraction / (1-sparkMemoryFraction));
          resetGCPools();
        } else {
          cnt++;
        }
      } 
      if(cnt % 3 == 2) {
        if(newRatio < (int) Math.ceil((1.0-DELTA)/DELTA)){
          double newOld = Math.min((1-DELTA)*heapSize - 1, oldSize + beta1.getMax());
          double newYoung = heapSize - newOld;
          newRatio = (int) Math.ceil(newOld / newYoung);
          resetGCPools();
        } else if(maxCores<=1 && sparkMemoryFraction - (beta1.getMax() / (heapSize - survivorRegion)) <= 0) {
System.out.println("No reliable configuration possible");
          break;
        }
      }
      cnt++;
//      sparkMemoryFraction = ((1-DELTA) * heapSize - beta0.getMax() - maxCores * beta1.getMax()) / (heapSize - survivorRegion);
//      int s = (int) Math.floor(10d * sparkMemoryFraction);
//      sparkMemoryFraction = Math.max(0.1, 0.1 * s);
    }
System.out.println("*New recommendation: DoP = " + maxCores);
System.out.println("*New recommendation: Spark memory = " + sparkMemoryFraction);
System.out.println("*New recommendation: NewRatio = " + newRatio);

/*
    if(beta0.getMax() + maxCores * beta1.getMax() + sparkMemoryFraction * (heapSize-survivorRegion) + youngSize > (1-DELTA) * heapSize) {
System.out.println("New ratio is too low: " + newRatio);
      double newYoung = ((1-DELTA) * heapSize - beta0.getMax() - maxCores * beta1.getMax() - sparkMemoryFraction * (heapSize-survivorRegion));
      double newOld = heapSize - newYoung;
      newRatio = (int) Math.ceil(newOld / newYoung);
System.out.println("*New recommendation: NewRatio = " + newRatio);
    }*/


  }

  public void recommend() {
System.out.println("Learned from configuration: " + maxCores + ", " + sparkMemoryFraction + ", " + newRatio);
    double initHeapSize = heapSize;
    double initSurvivorRegion = survivorRegion;
    int initMaxCores = maxCores;
    double defaultSparkMemoryFraction = sparkMemoryFraction;

    // increase storage or execution
    if(cacheHitRatio < 1) {
      sparkMemoryFraction = Math.min(0.9d, sparkMemoryFraction / cacheHitRatio);
    } else {
      sparkMemoryFraction = Math.min(0.9d, (1+DELTA)*(maxStorage.getMax() / (heapSize-survivorRegion)));
    }
    double initSparkMemoryFraction = sparkMemoryFraction;
    // old gen higher than spark
    newRatio = (int) Math.max(2.0, Math.ceil(sparkMemoryFraction / (1-sparkMemoryFraction)));
    resetGCPools();
//    }
    // cpu usage
    maxCores = (int) Math.ceil(75d / (avgCPU.getMax() / initMaxCores));

    double execMemory = sparkMemoryFraction;
    if(execSpillageFraction > 0) {
      execMemory = Math.min(0.9d, defaultSparkMemoryFraction / (1 - execSpillageFraction));
    } else {
      execMemory = Math.min(0.9d, (1+DELTA)*((beta1.getMax() - maxUnmgd.getMax()) / (heapSize-survivorRegion)));
    }
    maxCores = (int) Math.min(maxCores, (1.0 / execMemory) * initMaxCores);

    while(numExecs < 5) {
System.out.println("Calling reliablitity on: " + maxCores + ", " + sparkMemoryFraction + ", " + newRatio + ", " );
      makeReliable();
      // for execution heavy queries
      if(maxExecution.getMax() * maxCores / (initMaxCores * (initHeapSize-initSurvivorRegion)) > sparkMemoryFraction) {
        sparkMemoryFraction = Math.min(1.0 - (beta0.getMax() + maxUnmgd.getMax() * maxCores) / (initHeapSize-initSurvivorRegion), (1+DELTA)*(maxExecution.getMax() * maxCores / (initMaxCores * (initHeapSize-initSurvivorRegion))) / (1 - execSpillageFraction));
System.out.println("*New recommendation: Spark memory = " + sparkMemoryFraction);
      }
      numExecs++;
      heapSize = initHeapSize / numExecs;
      maxCores = (int) Math.ceil(75d / (avgCPU.getMax() / initMaxCores) / numExecs);
      maxCores = (int) Math.min(maxCores, (1.0 / execMemory) / initMaxCores / numExecs);
      if(maxCores < 1) {
System.out.println("Max cores lower than 1, exiting");
        break;
      }
      sparkMemoryFraction = initSparkMemoryFraction;
      newRatio = (int) Math.max(2.0, Math.ceil(sparkMemoryFraction / (1-sparkMemoryFraction)));
      resetGCPools();
    }
  }

  public static void main(String[] args) {
    String appId = "application_1537883326010_0298";
    if(args.length > 0) {
      appId = args[0];
    }
    SimpleBlackBoxModel model = new SimpleBlackBoxModel(appId);

    model.learn();
    model.recommend();
  }

}
