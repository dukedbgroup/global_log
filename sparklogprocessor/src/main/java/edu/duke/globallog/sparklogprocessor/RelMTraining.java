package edu.duke.globallog.sparklogprocessor;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * Class to combine stats from spark event logs and resource monitors and summarize them
 * Writes output to mysql db
 *
 */
public class RelMTraining
{

  public static void main(String[] args) {
    // connection to database
    Connection conn = null;

    final String DB_URL = "jdbc:mysql://localhost/test";
    final String DB_USER = "root";
    final String DB_PASSWORD = "database";

    final String TASK_METRICS_TABLE = "TASK_METRICS_ALL";
    final String IDENTITY_TABLE = "STAGE_IDENTITY";
    final String TASK_NUMBERS_TABLE = "TASK_NUMBERS";
    final String TASK_TIMES_TABLE = "TASK_TIMES";
    final String APP_ENV_TABLE = "APP_ENV";
    final String PERF_MONITORS_TABLE = "PERF_MONITORS";
    final String RELM_TABLE = "RELM_DATA";

    final String APP_ID = args[0];
    final String APP_NAME = args[1];
    // Since we aren't tracking amount of data storage in existing database, sending it as an additional parameter
    final Long MEMORY_STORAGE = Long.parseLong(args[2]);
    // Pass on Configurations since we are not saving it in database
    final Long maxHeap = Long.parseLong(args[3]);
    final Long maxCores = Long.parseLong(args[4]);
    final Long yarnOverhead = Long.parseLong(args[5]);
    final Long numExecs = Long.parseLong(args[6]);
    final Double sparkFraction = Double.parseDouble(args[7]);
    final Boolean offHeap = Boolean.parseBoolean(args[8]);
    final Long offHeapSize = Long.parseLong(args[9]);
    final String serializer = args[10];
    final String GCAlgo = args[11];
    final Long newRatio = Long.parseLong(args[12]);
    // HACK: parameter to find which stage caches data
    final Integer cacheStage = Integer.parseInt(args[13]);

    try {
      Class.forName("com.mysql.jdbc.Driver").newInstance();
      conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);

      Statement stmt = conn.createStatement();

      String sql1 = "CREATE TABLE IF NOT EXISTS " + RELM_TABLE + " (appId VARCHAR(255), " +
             "appName VARCHAR(255), stageId BIGINT, ipBytes BIGINT, cachedBytes BIGINT, " +
             "shuffleBytesRead BIGINT, shuffleBytesWritten BIGINT, " +
             "opBytes BIGINT, cacheStorage BIGINT, " + 
             "maxHeap BIGINT, maxCores BIGINT, yarnOverhead BIGINT, numExecs BIGINT, " + 
             "sparkMemoryFraction DECIMAL(4,2), offHeap BOOLEAN, offHeapSize BIGINT, " +
             "serializer VARCHAR(255), gcAlgo VARCHAR(255), newRatio BIGINT, " +
             "failedExecs BIGINT, maxStorage BIGINT, maxExecution BIGINT, totalTime BIGINT, " +
             "maxUsedHeap BIGINT, minUsageGap BIGINT, maxOldGenUsed BIGINT, " +
             "totalGCTime BIGINT, totalNumYoungGC BIGINT, totalNumOldGC BIGINT, " +
             "PRIMARY KEY(appId, stageId))";
      stmt.executeUpdate(sql1);

      String sql2 = "CREATE TABLE IF NOT EXISTS " + APP_ENV_TABLE + " (appId VARCHAR(255), " +
             "appName VARCHAR(255), startTime BIGINT, runTime BIGINT, " + 
             "maxHeap BIGINT, maxCores BIGINT, yarnOverhead BIGINT, numExecs BIGINT, " + 
             "sparkMemoryFraction DECIMAL(4,2), offHeap BOOLEAN, offHeapSize BIGINT, " +
             "serializer VARCHAR(255), gcAlgo VARCHAR(255), newRatio BIGINT, " +
             "PRIMARY KEY(appId))";
      stmt.executeUpdate(sql2);

      try { stmt.close(); } catch(Exception e) {}

      String qsql1 = "SELECT stageId as one, sum(ipBytesRead) as two, " + 
             "sum(shLocalBytesRead+shRemoteBytesRead) as three, sum(shBytesWritten) as four, " +
             "sum(opBytesWritten) as five from " + IDENTITY_TABLE + " where appId = \"" + APP_ID + 
             "\" GROUP BY stageId";
      String qsql2 = "SELECT sum(numTasks) as result FROM " + TASK_NUMBERS_TABLE + 
             " WHERE appId = \"" + APP_ID + "\" and stageId = ?";
      String qsql3 = "SELECT max(maxHeapUsed) as one, max(maxOldGenUsed) as two, " +
             "sum(numOldGC) as three, sum(numYoungGC) as four, " +
             "max(maxStorageUsed) as five, max(maxExecutionUsed) as six, " +
             "min(? - maxHeapUsed) as seven, sum(totalTime) as eight, count(1) as nine FROM " +
             PERF_MONITORS_TABLE + " WHERE appId = \"" + APP_ID + "\" and stageId = ?";
      String qsql4 = "SELECT sum(GCTime) as result FROM " + TASK_METRICS_TABLE + 
             " WHERE appId = \"" + APP_ID + "\" and stageId = ?";
      String qsql5 = "SELECT min(launchTime) as one, max(launchTime + runTime) as two FROM " +
             TASK_METRICS_TABLE + " WHERE appId = \"" + APP_ID + "\" and stageId = ?";

      String isql1 = "INSERT INTO " + RELM_TABLE + " values " +
             "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
      String isql2 = "INSERT INTO " + APP_ENV_TABLE + " values " +
             "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

      PreparedStatement qstmt1 = conn.prepareStatement(qsql1);
      PreparedStatement qstmt2 = conn.prepareStatement(qsql2);
      PreparedStatement qstmt3 = conn.prepareStatement(qsql3);
      PreparedStatement qstmt4 = conn.prepareStatement(qsql4);
      PreparedStatement qstmt5 = conn.prepareStatement(qsql5);
      PreparedStatement istmt1 = conn.prepareStatement(isql1);
      PreparedStatement istmt2 = conn.prepareStatement(isql2);

      conn.setAutoCommit(false);

      istmt2.clearParameters();
      istmt2.setObject(1, APP_ID);
      istmt2.setObject(2, APP_NAME);
      istmt2.setObject(5, maxHeap);
      istmt2.setObject(6, maxCores);
      istmt2.setObject(7, yarnOverhead);
      istmt2.setObject(8, numExecs);
      istmt2.setObject(9, sparkFraction);
      istmt2.setObject(10, offHeap);
      istmt2.setObject(11, offHeapSize);
      istmt2.setObject(12, serializer);
      istmt2.setObject(13, GCAlgo);
      istmt2.setObject(14, newRatio);
      Double startTime = Double.MAX_VALUE;
      Double endTime = Double.MIN_VALUE;

      ResultSet rs1 = qstmt1.executeQuery();
      while(rs1.next()) {
        String stage = rs1.getString("one");
        Double ipBytesRead = rs1.getDouble("two");
        Double shBytesRead = rs1.getDouble("three");
        Double shBytesWritten = rs1.getDouble("four");
        Double opBytesWritten = rs1.getDouble("five");

        Double numTasks = 0.0;
        qstmt2.clearParameters();
        qstmt2.setObject(1, stage);
        ResultSet rs2 = qstmt2.executeQuery();
        while(rs2.next()) {
          numTasks = rs2.getDouble("result");
        }

        istmt1.clearParameters();
        istmt1.setObject(1, APP_ID);
        istmt1.setObject(2, APP_NAME);
        istmt1.setObject(3, stage);
        istmt1.setObject(4, ipBytesRead / numTasks);
        // HACK: Assuming that 1st stage is cache stage always
        if(Integer.parseInt(stage) % cacheStage == 0) {
          istmt1.setObject(5, MEMORY_STORAGE / numTasks);
        } else {
          istmt1.setObject(5, 0L);
        }
        if(Integer.parseInt(stage) == 0) {
          istmt1.setObject(6, 0L);
        } else {
          istmt1.setObject(6, MEMORY_STORAGE);
        }
        istmt1.setObject(7, shBytesRead / numTasks);
        istmt1.setObject(8, shBytesWritten / numTasks);
        istmt1.setObject(9, opBytesWritten / numTasks);

        istmt1.setObject(10, maxHeap);
        istmt1.setObject(11, maxCores);
        istmt1.setObject(12, yarnOverhead);
        istmt1.setObject(13, numExecs);
        istmt1.setObject(14, sparkFraction);
        istmt1.setObject(15, offHeap);
        istmt1.setObject(16, offHeapSize);
        istmt1.setObject(17, serializer);
        istmt1.setObject(18, GCAlgo);
        istmt1.setObject(19, newRatio);

        qstmt3.clearParameters();
        qstmt3.setObject(2, stage);
        qstmt3.setObject(1, maxHeap);
        ResultSet rs3 = qstmt3.executeQuery();
        while(rs3.next()) {
          Long numFailedExecs = Math.max(rs3.getLong("nine") - numExecs, 0);
          istmt1.setObject(19, numFailedExecs);
          istmt1.setObject(20, rs3.getDouble("five"));
          istmt1.setObject(21, rs3.getDouble("six"));
          istmt1.setObject(22, rs3.getDouble("eight"));
          istmt1.setObject(23, rs3.getDouble("one"));
          istmt1.setObject(24, rs3.getDouble("seven"));
          istmt1.setObject(26, rs3.getDouble("two"));
          istmt1.setObject(27, rs3.getDouble("four"));
          istmt1.setObject(28, rs3.getDouble("three"));
        }
 
        qstmt4.clearParameters();
        qstmt4.setObject(1, stage);
        ResultSet rs4 = qstmt4.executeQuery();
        while(rs4.next()) {
          istmt1.setObject(25, rs4.getDouble("result"));
        }

        qstmt5.clearParameters();
        qstmt5.setObject(1, stage);
        ResultSet rs5 = qstmt5.executeQuery();
        while(rs5.next()) {
          Double minTime = rs5.getDouble("one");
          Double maxTime = rs5.getDouble("two");
          if(minTime < startTime) {
            startTime = minTime;
          }
          if(maxTime > endTime) {
            endTime = maxTime;
          }
        }

        istmt1.addBatch();
      }

      istmt2.setObject(3, startTime);
      istmt2.setObject(4, endTime-startTime);
      istmt2.addBatch();

System.out.println("--Running " + istmt1);
System.out.println("--Running " + istmt2);
      istmt1.executeBatch();
      istmt2.executeBatch();
      conn.commit();

      try { qstmt1.close(); } catch(Exception e) {}
      try { qstmt2.close(); } catch(Exception e) {}
      try { qstmt3.close(); } catch(Exception e) {}
      try { qstmt4.close(); } catch(Exception e) {}
      try { qstmt5.close(); } catch(Exception e) {}
      try { istmt1.close(); } catch(Exception e) {}
      try { istmt2.close(); } catch(Exception e) {}

    } 
    catch(Exception e) {
          e.printStackTrace();
    } finally {
          try { conn.close(); } catch(Exception e) {}
    }
  }
}
