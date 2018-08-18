package edu.duke.globallog.sparklogprocessor;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Class to combine stats from spark event logs and resource monitors and summarize them
 * Writes output to mysql db
 *
 */
public class TrainRelM
{

  // utility function to parse command line string showing flowgraph
  // e.g. "2->1|3->1,2|4->3"
  static Map<String, Set<String>> parseParentsMap(String mapString) {
    return Arrays.stream(mapString.split("#"))
      .map(s -> s.split("->"))
      .collect(Collectors.toMap(
         a -> a[0],
         a -> Arrays.stream(a[1].split(",")).collect(Collectors.toCollection(LinkedHashSet::new)),
         (u, v) -> u,
         LinkedHashMap::new
      ));
  }

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
    final String RELM_TABLE = "RELM";

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
    final Map<String, Set<String>> parents = parseParentsMap(args[13]);

    try {
      Class.forName("com.mysql.jdbc.Driver").newInstance();
      conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);

      Statement stmt = conn.createStatement();

      String sql1 = "CREATE TABLE IF NOT EXISTS " + RELM_TABLE + " (appId VARCHAR(255), " +
             "appName VARCHAR(255), stageId BIGINT, ipBytes BIGINT, cachedBytes BIGINT, " +
             "shuffleBytesRead BIGINT, shuffleBytesWritten BIGINT, " +
             "opBytes BIGINT, cacheBytesRead BIGINT, " + 
             "maxHeap BIGINT, maxCores BIGINT, yarnOverhead BIGINT, numExecs BIGINT, " + 
             "sparkMemoryFraction DECIMAL(4,2), offHeap BOOLEAN, offHeapSize BIGINT, " +
             "serializer VARCHAR(255), gcAlgo VARCHAR(255), newRatio BIGINT, " +
             "failedExecs BIGINT, numTasks BIGINT, failedTasks BIGINT, " +
             "pLocalTasks BIGINT, diskBytesSpilled BIGINT, " + 
             "maxStorage BIGINT, maxExecution BIGINT, " + 
             "maxUsedHeap BIGINT, minUsageGap BIGINT, maxOldGenUsed BIGINT, " +
             "totalNumYoungGC BIGINT, totalNumOldGC BIGINT, " +
             "mTaskTime DECIMAL(20,2), mGCTime DECIMAL(20,2), mDesTime DECIMAL(20,2), mSerTime DECIMAL(20,2), " +
             "reliability DECIMAL(20,2), efficiency DECIMAL(20,2), perf1 DECIMAL(20,2), perf2 DECIMAL(20,2), " +
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

      String qsql1 = "SELECT stageId as one, avg(ipBytesRead) as two, " + 
             "avg(shLocalBytesRead+shRemoteBytesRead) as three, avg(shBytesWritten) as four, " +
             "avg(opBytesWritten) as five from " + TASK_METRICS_TABLE + " where appId = \"" +
             APP_ID + "\" and taskId in (select taskId from " + TASK_METRICS_TABLE +
             " where appId=\"" + APP_ID + "\" " +
             "group by taskId having count(1)=1) GROUP BY stageId";
//      String qsql2 = "SELECT sum(numTasks) as nTasks, sum(failedTasks) as fTasks, " +
//             "sum(processLocalTasks) as lTasks FROM " + TASK_NUMBERS_TABLE + 
//             " WHERE appId = \"" + APP_ID + "\" and stageId = ?";
      String qsql3 = "SELECT max(maxStorageUsed) as one, max(maxExecutionUsed) as two, " +
             "max(maxHeapUsed) as three, min(? - maxHeapUsed) as four, " +
             "max(maxOldGenUsed) as five, " +
             "sum(numYoungGC) as six, sum(numOldGC) as seven, " +
             "sum(totalTime) as eight, count(1) as nine FROM " +
             PERF_MONITORS_TABLE + " WHERE appId = \"" + APP_ID + "\" and stageId = ?";
      String qsql4 = "SELECT avg(finishTime-launchTime) as tTime, avg(GCTime) as gTime, " +
             "avg(deserializeTime) as dTime, avg(serializeTime) as sTime FROM " +
             TASK_METRICS_TABLE + 
             " WHERE appId = \"" + APP_ID + "\" and stageId = ?" +
             " and taskId in (select taskId from " + TASK_METRICS_TABLE +
             " where appId=\"" + APP_ID + "\" and stageId = ? " +
             "group by taskId having count(1)=1)";
      String qsql5 = "SELECT min(launchTime) as one, max(finishTime) as two, " +
             "sum(diskSpilled) as three FROM " +
             TASK_METRICS_TABLE + " WHERE appId = \"" + APP_ID + "\" and stageId = ?" +
             " and taskId in (select taskId from " + TASK_METRICS_TABLE +
             " where appId=\"" + APP_ID + "\" and stageId = ? " +
             "group by taskId having count(1)=1)";
      String qsql6 = "select count(1) as cnt from " + TASK_METRICS_TABLE + " where appId=\"" +
             APP_ID + "\" and stageId = ? and taskId in (select taskId from " +
             TASK_METRICS_TABLE + " where appId=\"" + APP_ID + "\" and stageId = ? " +
             "group by taskId having count(1)=1)";
      String qsql7 = "select count(1) as cnt from " + TASK_METRICS_TABLE + " where appId=\"" +
             APP_ID + "\" and stageId = ? and taskId in (select taskId from " +
             TASK_METRICS_TABLE + " where appId=\"" + APP_ID +
             "\" and stageId = ? and failed = 1 " +
             "group by taskId having count(1)=1)";
      String qsql8 = "select count(1) as cnt, avg(ipBytesRead) as bytes " +
             " from " + TASK_METRICS_TABLE + " where appId=\"" + APP_ID +
             "\" and stageId = ? and taskId in (select taskId from " + TASK_METRICS_TABLE + 
             " where appId=\"" + APP_ID + "\" and stageId = ? and locality = \'process_local\' " +
             "group by taskId having count(1)=1)";

      String isql1 = "INSERT INTO " + RELM_TABLE + " values " +
             "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
      String isql2 = "INSERT INTO " + APP_ENV_TABLE + " values " +
             "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

      String usql1 = "UPDATE " + RELM_TABLE + " SET cachedBytes = ? " + 
             "WHERE appId = \"" + APP_ID + "\" and stageId = ?";

      PreparedStatement qstmt1 = conn.prepareStatement(qsql1);
//      PreparedStatement qstmt2 = conn.prepareStatement(qsql2);
      PreparedStatement qstmt3 = conn.prepareStatement(qsql3);
      PreparedStatement qstmt4 = conn.prepareStatement(qsql4);
      PreparedStatement qstmt5 = conn.prepareStatement(qsql5);
      PreparedStatement qstmt6 = conn.prepareStatement(qsql6);
      PreparedStatement qstmt7 = conn.prepareStatement(qsql7);
      PreparedStatement qstmt8 = conn.prepareStatement(qsql8);
      PreparedStatement istmt1 = conn.prepareStatement(isql1);
      PreparedStatement istmt2 = conn.prepareStatement(isql2);
      PreparedStatement ustmt1 = conn.prepareStatement(usql1);

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

      // need the following maps to find bytes written by stages
      Map<String, Double> cacheReadByStage = new LinkedHashMap<String, Double>();
      Map<String, Double> tasksByStage = new LinkedHashMap<String, Double>();

      ResultSet rs1 = qstmt1.executeQuery();
      while(rs1.next()) {
        String stage = rs1.getString("one");
        Double ipBytesRead = rs1.getDouble("two");
        Double shBytesRead = rs1.getDouble("three");
        Double shBytesWritten = rs1.getDouble("four");
        Double opBytesWritten = rs1.getDouble("five");
        Double cacheBytesRead = 0.0;

        Double numTasks = 0.0;
        Double failedTasks = 0.0;
        Double pLocalTasks = 0.0;
        Double diskSpilled = 0.0;
        qstmt6.clearParameters();
        qstmt6.setObject(1, stage);
        qstmt6.setObject(2, stage);
        ResultSet rs6 = qstmt6.executeQuery();
        while(rs6.next()) {
          numTasks = rs6.getDouble("cnt");
        }
        qstmt7.clearParameters();
        qstmt7.setObject(1, stage);
        qstmt7.setObject(2, stage);
        ResultSet rs7 = qstmt7.executeQuery();
        while(rs7.next()) {
          failedTasks = rs7.getDouble("cnt");
        }
        qstmt8.clearParameters();
        qstmt8.setObject(1, stage);
        qstmt8.setObject(2, stage);
        ResultSet rs8 = qstmt8.executeQuery();
        while(rs8.next()) {
          pLocalTasks = rs8.getDouble("cnt");
          cacheBytesRead = rs8.getDouble("bytes");
        }
        qstmt5.clearParameters();
        qstmt5.setObject(1, stage);
        qstmt5.setObject(2, stage);
        ResultSet rs5 = qstmt5.executeQuery();
        while(rs5.next()) {
          startTime = rs5.getDouble("one");
          endTime = rs5.getDouble("two");
          diskSpilled = rs5.getDouble("three");
        }

        istmt1.clearParameters();
        istmt1.setObject(1, APP_ID);
        istmt1.setObject(2, APP_NAME);
        istmt1.setObject(3, stage);
        // HACK: Assuming that 1st stage is cache stage always
        istmt1.setObject(5, 0.0); // dummy set, ustmt1 will set it right
/*        if(Integer.parseInt(stage) % cacheStage == 0) {
          istmt1.setObject(5, MEMORY_STORAGE / numTasks);
        } else {
          istmt1.setObject(5, 0L);
        }
        if(Integer.parseInt(stage) == 0) {
          istmt1.setObject(29, 0L);
        } else {
          istmt1.setObject(29, MEMORY_STORAGE);
        }
*/
        istmt1.setObject(6, shBytesRead);
        istmt1.setObject(7, shBytesWritten);
        istmt1.setObject(8, opBytesWritten);

        Double cacheRead = 0.0;
        if(parents.containsKey(stage)) {
          if(shBytesRead > 0.0) { // shuffle read stage
            istmt1.setObject(4, shBytesRead);
            istmt1.setObject(9, 0.0);
            istmt1.setObject(38, 1.0 - diskSpilled / shBytesRead); // perf1
          } else { // cached RDD read
            if(cacheBytesRead > 0.0) {
              istmt1.setObject(4, cacheBytesRead);
              istmt1.setObject(9, cacheBytesRead);
            } else { // pLocalTasks will be 0 here
              istmt1.setObject(4, ipBytesRead);
              istmt1.setObject(9, 0.0);
            }
            istmt1.setObject(38, pLocalTasks / numTasks); // perf1
            cacheReadByStage.put(stage, cacheBytesRead * numTasks); // saving total cache size
          }
        } else {
          istmt1.setObject(4, ipBytesRead);
          istmt1.setObject(9, 0.0);
          istmt1.setObject(38, 1.0); // perf1
        }

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
          istmt1.setObject(20, numFailedExecs);
          istmt1.setObject(36, Math.max(0, 1.0 - numFailedExecs.doubleValue() / numExecs)); // reliability
          istmt1.setObject(25, rs3.getDouble("one")); // maxStorage
          istmt1.setObject(26, rs3.getDouble("two")); // maxExecution
          Double maxHeapUsed = rs3.getDouble("three");
          istmt1.setObject(27, maxHeapUsed);
          istmt1.setObject(37, maxHeapUsed / maxHeap); // efficiency
          istmt1.setObject(28, rs3.getDouble("four"));
          istmt1.setObject(29, rs3.getDouble("five"));
          istmt1.setObject(30, rs3.getDouble("six"));
          istmt1.setObject(31, rs3.getDouble("seven"));
//          istmt1.setObject(32, rs3.getDouble("eight"));
        }

        istmt1.setObject(21, numTasks);
        istmt1.setObject(22, failedTasks);
        istmt1.setObject(23, pLocalTasks);
        istmt1.setObject(24, diskSpilled);

        tasksByStage.put(stage, numTasks);
 
        qstmt4.clearParameters();
        qstmt4.setObject(1, stage);
        qstmt4.setObject(2, stage);
        ResultSet rs4 = qstmt4.executeQuery();
        while(rs4.next()) {
          Double tTime = rs4.getDouble("tTime");
          Double gTime = rs4.getDouble("gTime");
          istmt1.setObject(32, tTime);
          istmt1.setObject(33, gTime);
          istmt1.setObject(39, 1 - gTime / tTime); // perf2
          istmt1.setObject(34, rs4.getDouble("dTime"));
          istmt1.setObject(35, rs4.getDouble("sTime"));
        }

System.out.println("--Adding to batch: " + istmt1);
        istmt1.addBatch();
      }

      istmt2.setObject(3, startTime);
      istmt2.setObject(4, endTime-startTime);
//      istmt2.addBatch();

System.out.println("--Running " + istmt2);
      istmt1.executeBatch();
//      istmt2.executeBatch();
      conn.commit();

      // update cached bytes
      Map<String, Double> cachedBytesPerStage = new LinkedHashMap<String, Double>();
      for(String stage: parents.keySet()) {
        Double readBytes = cacheReadByStage.getOrDefault(stage, 0.0);
        for(String source: parents.get(stage)) {
          if(cachedBytesPerStage.containsKey(source)) {
            readBytes = Math.max(0, readBytes - cachedBytesPerStage.get(source));
          } else {
            Double tasks = tasksByStage.getOrDefault(source, 1.0);
            ustmt1.clearParameters();
            ustmt1.setObject(1, readBytes / tasks);
            ustmt1.setObject(2, source);
            ustmt1.addBatch();
            cachedBytesPerStage.put(source, readBytes);
          }
        }
      }
System.out.println("--Running " + ustmt1);
      ustmt1.executeBatch();
      conn.commit();

      try { qstmt1.close(); } catch(Exception e) {}
//      try { qstmt2.close(); } catch(Exception e) {}
      try { qstmt3.close(); } catch(Exception e) {}
      try { qstmt4.close(); } catch(Exception e) {}
      try { qstmt5.close(); } catch(Exception e) {}
      try { qstmt6.close(); } catch(Exception e) {}
      try { qstmt7.close(); } catch(Exception e) {}
      try { qstmt8.close(); } catch(Exception e) {}
      try { istmt1.close(); } catch(Exception e) {}
      try { istmt2.close(); } catch(Exception e) {}
      try { ustmt1.close(); } catch(Exception e) {}

    } 
    catch(Exception e) {
          e.printStackTrace();
    } finally {
          try { conn.close(); } catch(Exception e) {}
    }
  }
}
