package edu.duke.globallog.sparklogprocessor;

import java.nio.file.*;
import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.stream.*;

import cern.colt.matrix.tdouble.*;
import cern.colt.matrix.tdouble.*;
import cern.colt.matrix.tint.*;

public class WIFTester extends TrainingBase {

  String resultsPath = "cluster-results"; // default
  RegressionResults model;

  // configuration
  double maxHeap;
  double sparkMemoryFraction;
  double sparkStorageFraction;
  long maxCores;
  long newRatio;
  long survivorRatio;
  long numTasks, numExecs;

  // stage id
  double ipBytes = 0d, cachedBytes = 0d, cacheBytesRead = 0d;
  double shBytesWritten = 0d, shBytesRead = 0d, opBytes = 0d;

  // pools
  double sparkMemPoolSize;
  double oldGenSize, youngGenSize;
  double storageReq, execReq;

  // evaluated y values
  double surge, shortterm, storage;
  double surge_sd, shortterm_sd, storage_sd;

  WIFTester(int cluster) {
    extractModel(cluster);
    maxHeap = 2.0 * 1024 * 1024 * 1024;
    sparkMemoryFraction = 0.6;
    sparkStorageFraction = 0.5;
    maxCores = 1;
    newRatio = 2;
    survivorRatio = 8;
    // resetPools();
  }

  private void extractModel(int cluster) {
    String file = resultsPath + "/params-" + cluster + ".ser";
    FileInputStream fin = null;
    ObjectInputStream ois = null;
    try {
      fin = new FileInputStream(file);
      ois = new ObjectInputStream(fin);
      model = (RegressionResults) ois.readObject();
System.out.println("Extracted model: " + model);
    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
      if (fin != null) {
        try { fin.close(); } catch(Exception e) {}
      }
      if (ois != null) {
        try { ois.close(); } catch(Exception e) {}
      }
    }
  }

  private void resetPools() {
    oldGenSize = maxHeap * newRatio / (newRatio + 1);
    youngGenSize = maxHeap * (survivorRatio - 1) / 
                   survivorRatio / (newRatio + 1);
    sparkMemPoolSize = maxHeap * sparkMemoryFraction * 
                       (1 - (1 / survivorRatio / (newRatio + 1)));
System.out.println("Pool sizes: old->" + oldGenSize + " young->" + youngGenSize + " spark->" + sparkMemPoolSize);

    storageReq = cacheBytesRead * numTasks / numExecs;
    execReq = 0d;
System.out.println("Storage req->" + storageReq + " Exec req->" + execReq);

    evaluateModels();
  }

  private void evaluateModels() {
    // DoubleMatrix1D means = model.getMeans();
    // DoubleMatrix2D coVar = model.getCoVar();
    // Hack: hardcode model since above returning null values
    DoubleMatrix1D means = DoubleFactory1D.dense.make(new double[]{
      6.34E9, 11.79, -32.68, 12.61, 5.2E8, 0.34, 0.52, 1.18E7, 4660889
    });
    DoubleMatrix2D coVar = DoubleFactory2D.dense.make(new double[][] {
      {2.5E18, 2.9E9, -1.4E11, -1.7E9, -3.6E16, 0, 0, 0, 0},
      {2.9E9, 76.5, -3783.7, -12.9, 4.1E8, 0, 0, 0, 0},
      {-1.4E11, -3783.7, 187566.3, 627.8, -2.07, 0, 0, 0, 0},
      {-1.7E9, -12.9, 627.8, 18.66, -6.8E8, 0, 0, 0, 0},
      {-3.6E16, 4.1E8, -2.07, -6.8E8, 2.97E18, 0, 0, 0, 0},
      {0, 0, 0, 0, 0, 9.8E-5, -3.3E-5, 0, 0},
      {0, 0, 0, 0, 0, -3.3E-5, 5.26E-5, 0, 0},
      {0, 0, 0, 0, 0, 0, 0, 1.39E14, -4.9E13},
      {0, 0, 0, 0, 0, 0, 0, -4.9E13, 2.17E13}
    });

    // short term
    double pLocalRatio = Math.max(1.0, sparkMemPoolSize / storageReq);
    double totalIpBytes = (1 - pLocalRatio) * numTasks * ipBytes / numExecs;
    double totalCacheBytes = pLocalRatio * numTasks * cacheBytesRead / numExecs;
    // hacked for micro stage 2 only
    double totalOpBytes = ((1 - pLocalRatio) * 2700000 + pLocalRatio * 2700) * numTasks / numExecs;
    // hacke for kmeans stage 5, 7, .. only
    // double totalOpBytes = 2043 * numTasks / numExecs;
    double totalShWrite = shBytesWritten * numTasks / numExecs;
    double totalShRead = shBytesRead * numTasks / numExecs;
    shortterm = means.get(0) + means.get(1) * totalIpBytes + 
                // means.get(2) * totalShRead +
                means.get(2) * totalOpBytes +
                means.get(3) * totalShWrite +
                means.get(4) * numTasks / numExecs;
    shortterm_sd = Math.pow(coVar.get(0,0), 2) +
                Math.pow(totalIpBytes, 2) * Math.pow(coVar.get(1,1), 2) +
                // Math.pow(totalShRead, 2) * Math.pow(coVar.get(2,2), 2) +
                Math.pow(totalOpBytes, 2) * Math.pow(coVar.get(2,2), 2) +
                Math.pow(totalShWrite, 2) * Math.pow(coVar.get(3,3), 2) +
                Math.pow(numTasks/numExecs, 2) * Math.pow(coVar.get(4,4), 2) +
                2 * totalIpBytes * coVar.get(0,1) +
                2 * totalOpBytes * coVar.get(0,2) +
                2 * totalShWrite * coVar.get(0,3) +
                2 * numTasks/numExecs * coVar.get(0,4) +
                2 * totalIpBytes * totalOpBytes * coVar.get(1,2) +
                2 * totalIpBytes * totalShWrite * coVar.get(1,3) +
                2 * totalIpBytes * numTasks/numExecs * coVar.get(1,4) +
                2 * totalOpBytes * totalShWrite * coVar.get(2,3) +
                2 * totalOpBytes * numTasks/numExecs * coVar.get(2,4) +
                2 * totalShWrite * numTasks/numExecs * coVar.get(3,4);
     shortterm_sd = Math.sqrt(shortterm_sd);
System.out.println("Found shortterm memory with 90% confidence: " + shortterm + "+-" + 2*shortterm_sd);

     // surge
     surge = means.get(7) + maxCores * means.get(8);
     surge_sd = Math.pow(coVar.get(7,7), 2) +
                Math.pow(coVar.get(8,8), 2) +
                2 * maxCores * coVar.get(7,8);
     surge_sd = Math.sqrt(surge_sd);
System.out.println("Found surge memory with 90% confidence: " + surge + "+-" + 2*surge_sd);

  }

  double computeR() {
    return 1.0;
  }

  double computeE() {
    double longterm = Math.max(sparkMemPoolSize, storageReq);
    double e = (surge + longterm) / maxHeap;
    return e;
  }

  double computeP1() {
    return 1.0;
  }

  double computeP2() {
    return 1.0;
  }

  void changeConfig() {
    // HACK: going to hardcode

    resetPools();
  }

  void extractConfig(String appId, long stageId) {
    String qsql1 = "SELECT maxHeap as one, maxCores as two, "
      + "sparkMemoryFraction as three, newRatio as four, "
      + "ipBytes as five, cachedBytes as six, "
      + "shuffleBytesWritten as seven, shuffleBytesRead as eight, "
      + "opBytes as nine, cacheBytesRead as ten, "
      + "numTasks as elevan, numExecs as twelve"
      + " FROM " + RELM_TABLE + " WHERE appId=? and stageId=?";
    try {
      startConnection();
      PreparedStatement qstmt1 = newPreparedStatement(qsql1);
      qstmt1.clearParameters();
      qstmt1.setObject(1, appId);
      qstmt1.setObject(2, stageId);
      ResultSet rs1 = qstmt1.executeQuery();
      if(rs1.next()) {
        maxHeap = rs1.getDouble("one");
        maxCores = rs1.getLong("two");
        sparkMemoryFraction = rs1.getDouble("three");
        newRatio = rs1.getLong("four");

        ipBytes = rs1.getDouble("five");
        cachedBytes = rs1.getDouble("six");
        shBytesWritten = rs1.getDouble("seven");
        shBytesRead = rs1.getDouble("eight");
        opBytes = rs1.getDouble("nine");
        cacheBytesRead = rs1.getDouble("ten");

        numTasks = rs1.getLong("elevan");
        numExecs = rs1.getLong("twelve");
      }
      closeStatement(qstmt1);
    } catch (Exception e) {
          e.printStackTrace();
    } finally {
      stopConnection();
    }

    resetPools();
  }

  public static void main(String[] args) {
    int clusterId = 26;
    if(args.length > 1) {
      clusterId = Integer.parseInt(args[0]);
    }
    WIFTester tester = new WIFTester(clusterId);
    String appId = "";
    if(args.length > 2) {
      appId = args[1];
    }
    long stageId = 2L;
    if(args.length > 3) {
      stageId = Long.parseLong(args[2]);
    }

    tester.extractConfig(appId, stageId);

//    tester.changeConfig(); // hack to try unseen config
    tester.computeR();
    tester.computeE();
    tester.computeP1();
    tester.computeP2();
  }

}
