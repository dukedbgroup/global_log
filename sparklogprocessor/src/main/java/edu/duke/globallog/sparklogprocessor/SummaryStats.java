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
public class SummaryStats
{

  public static void main(String[] args) {
    // connection to database
    Connection conn = null;

    final String DB_URL = "jdbc:mysql://localhost/test";
    final String DB_USER = "root";
    final String DB_PASSWORD = "database";

    final String PERF_MONITOR_HOME = "/home/mayuresh/heap-logs/";
    final String PERF_FILE_PREFIX = "sparkOutput_worker_";

    final String TASK_METRICS_TABLE = "TASK_METRICS_ALL";
    final String IDENTITY_TABLE = "STAGE_IDENTITY";
    final String TASK_NUMBERS_TABLE = "TASK_NUMBERS";
    final String TASK_TIMES_TABLE = "TASK_TIMES";
    final String APP_ENV_TABLE = "APP_ENV";
    final String PERF_MONITORS_TABLE = "PERF_MONITORS";

    final String APP_ID = args[0];

    try {
      Class.forName("com.mysql.jdbc.Driver").newInstance();
      conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);

      Statement stmt = conn.createStatement();

      String sql1 = "CREATE TABLE IF NOT EXISTS " + IDENTITY_TABLE + " (appId VARCHAR(255), " +
             "stageId BIGINT, executorId VARCHAR(8), host VARCHAR(255), " +
             "ipBytesRead BIGINT, ipRecordsRead BIGINT, " +
             "shRemoteBytesRead BIGINT, shLocalBytesRead BIGINT, shRecordsRead BIGINT, " +
             "opBytesWritten BIGINT, opRecordsWritten BIGINT, " +
             "shBytesWritten BIGINT, shRecordsWritten BIGINT, " +
             "PRIMARY KEY(appId, stageId, executorId))";
      stmt.executeUpdate(sql1);

      String sql2 = "CREATE TABLE IF NOT EXISTS " + TASK_TIMES_TABLE + " (appId VARCHAR(255), " +
             "stageId BIGINT, executorId VARCHAR(8), host VARCHAR(255), " +
             "avgTaskTime BIGINT, maxTaskTime BIGINT, avgDeserTime BIGINT, maxDeserTime BIGINT, " + 
             "avgResultTime BIGINT, maxResultTime BIGINT, avgGCTime BIGINT, maxGCTime BIGINT, " + 
             "shFetchWaitTime BIGINT, shWriteTime BIGINT, " +
             "PRIMARY KEY(appId, stageId, executorId))";
      stmt.executeUpdate(sql2);

      String sql3 = "CREATE TABLE IF NOT EXISTS " + TASK_NUMBERS_TABLE + " (appId VARCHAR(255), " +
             "stageId BIGINT, executorId VARCHAR(8), numTasks BIGINT, " +
             "numFailed BIGINT, numProcessLocal BIGINT, numNodeLocal BIGINT, numRackLocal BIGINT, " +
             "avgResultSize BIGINT, maxResultSize BIGINT, avgSpillMem BIGINT, maxSpillMem BIGINT, " +
             "PRIMARY KEY(appId, stageId, executorId))";
      stmt.executeUpdate(sql3);

      String sql4 = "CREATE TABLE IF NOT EXISTS " + APP_ENV_TABLE + " (appId VARCHAR(255), " +
             "appName VARCHAR(255), startTime datetime, runTime DECIMAL(6,2), " + 
             "maxHeap BIGINT, maxCores BIGINT, yarnOverhead BIGINT, numExecs BIGINT, " + 
             "sparkMemoryFraction DECIMAL(4,2), offHeap BOOLEAN, offHeapSize BIGINT, " +
             "serializer VARCHAR(255), gcAlgo VARCHAR(255), newRatio BIGINT, " +
             "PRIMARY KEY(appId))";
      stmt.executeUpdate(sql4);

      String sql5 = "CREATE TABLE IF NOT EXISTS " + PERF_MONITORS_TABLE + " (appId VARCHAR(255), " +
             "stageId BIGINT, executorId VARCHAR(8), host VARCHAR(255), " +
             "maxHeapUsed BIGINT, maxRSSUsed BIGINT, maxCPUUsed BIGINT, " +
             "maxOldGenUsed BIGINT, numOldGC BIGINT, numYoungGC BIGINT, " +
             "totalOldCollection BIGINT, totalYoungCollection BIGINT, " +
             "maxStorageUsed BIGINT, maxExecutionUsed BIGINT, " + 
             "totalTime BIGINT, numSamples BIGINT, " +
             "PRIMARY KEY(appId, stageId, executorId))";
      stmt.executeUpdate(sql5);

      try { stmt.close(); } catch(Exception e) {}

//      String qsql1 = "SELECT distinct(stageId) as result from " + TASK_METRICS_TABLE + " where appId = " + APP_ID;
      String qsql2 = "SELECT distinct(executorId) as result FROM " + TASK_METRICS_TABLE + " WHERE appId = \"" + APP_ID + "\"";

//      PreparedStatement qstmt1 = conn.prepareStatement(qsql1);
      PreparedStatement qstmt2 = conn.prepareStatement(qsql2);

//      List<String> stages = new ArrayList<String>();
//      ResultSet rs1 = qstmt1.executeQuery();
//      while(rs1.next()) {
//        stages.add(rs1.getString("result"));
//      }

      List<String> execs = new ArrayList<String>();
      ResultSet rs2 = qstmt2.executeQuery();
      while(rs2.next()) {
        execs.add(rs2.getString("result"));
      }

//      try { qstmt1.close(); } catch(Exception e) {}
      try { qstmt2.close(); } catch(Exception e) {}

      String qsql3 = "SELECT max(host) as one, sum(ipBytesRead) as two, sum(ipRecordsRead) as three, " +
             "sum(shRemoteBytesRead) as four, sum(shLocalBytesRead) as five, " + 
             "sum(shRecordsRead) as six, sum(opBytesWritten) as seven, " +
             "sum(opRecordsWritten) as eight, " +
             "sum(shBytesWritten) as nine, sum(shRecordsWritten) as ten, " +
             "avg(finishTime-launchTime) as elevan, max(finishTime-launchTime) as twelve, " +
             "avg(deserializeTime) as thirteen, max(deserializeTime) as fourteen, " +
             "avg(resultTime) as fifteen, max(resultTime) as sixteen, " +
             "avg(GCTime) as seventeen, max(GCTime) as eighteen, " +
             "sum(shFetchWaitTime) as nineteen, sum(shWriteTime) as twenty," +
             "count(1) as twoone, avg(resultSize) as twotwo, max(resultSize) as twothree, " +
             "avg(memorySpilled) as twofour, max(memorySpilled) as twofive " +
             "FROM " + TASK_METRICS_TABLE + 
             " WHERE appId = ? AND stageId = ? AND executorId = ?";
      PreparedStatement qstmt3 = conn.prepareStatement(qsql3);

      String isql1 = "INSERT INTO " + IDENTITY_TABLE + " values " +
             "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
      String isql2 = "INSERT INTO " + TASK_TIMES_TABLE + " values " +
             "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
      String isql3 = "INSERT INTO " + TASK_NUMBERS_TABLE + " values " +
             "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
      String isql4 = "INSERT INTO " + PERF_MONITORS_TABLE + " values " +
             "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
      String isql5 = "INSERT INTO " + APP_ENV_TABLE + " values " +
             "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

      String qsql4 = "SELECT count(1) as count from " + TASK_METRICS_TABLE + 
             " WHERE failed = TRUE AND appId = ? AND stageId = ? AND executorId = ?";
      String qsql5 = "SELECT count(1) as count from " + TASK_METRICS_TABLE +
             " WHERE locality=\'PROCESS_LOCAL\' AND appId = ? AND stageId = ? AND executorId = ?";
      String qsql6 = "SELECT count(1) as count from " + TASK_METRICS_TABLE +
             " WHERE locality=\'NODE_LOCAL\' AND appId = ? AND stageId = ? AND executorId = ?";
      String qsql7 = "SELECT count(1) as count from " + TASK_METRICS_TABLE +
             " WHERE locality=\'RACK_LOCAL\' AND appId = ? AND stageId = ? AND executorId = ?";
      String qsql8 = "SELECT stageId, (max(finishTime)-min(launchTime)) as stageTime from " +
             TASK_METRICS_TABLE + " WHERE appId = ? AND executorId = ? group by stageId";

      PreparedStatement qstmt4 = conn.prepareStatement(qsql4);
      PreparedStatement qstmt5 = conn.prepareStatement(qsql5);
      PreparedStatement qstmt6 = conn.prepareStatement(qsql6);
      PreparedStatement qstmt7 = conn.prepareStatement(qsql7);
      PreparedStatement qstmt8 = conn.prepareStatement(qsql8);
      PreparedStatement istmt1 = conn.prepareStatement(isql1);
      PreparedStatement istmt2 = conn.prepareStatement(isql2);
      PreparedStatement istmt3 = conn.prepareStatement(isql3);
      PreparedStatement istmt4 = conn.prepareStatement(isql4);
      PreparedStatement istmt5 = conn.prepareStatement(isql5);

      conn.setAutoCommit(false);

      for(String exec: execs) {
        // hard-coding file path pattern
        String fileToRead = PERF_MONITOR_HOME + APP_ID + "/" + exec + "/" + PERF_FILE_PREFIX + APP_ID + "_" + exec + ".txt";
        Long totalLines = Files.lines(Paths.get(fileToRead)).count() - 2; //first two header lines

        Map<String, Double> stageTimeMap = new HashMap<String, Double>();
        // Map<String, Long> stageStartLineMap = new HashMap<String, Long>();
        Map<String, Long> stageNumLinesMap = new HashMap<String, Long>();
        Double totalTime = 0d;
        qstmt8.clearParameters();
        qstmt8.setObject(1, APP_ID);
        qstmt8.setObject(2, exec);
        ResultSet rs8 = qstmt8.executeQuery();
        while(rs8.next()) {
          String s = rs8.getString("stageId");
          Double t = rs8.getDouble("stageTime");
          stageTimeMap.put(s, t);
          totalTime += t;
        }
        // Long startLine = 2L;
        for(String stage: stageTimeMap.keySet()) {
          // stageStartLineMap.put(stage, startLine);
          Long numLines = Math.round(totalLines * stageTimeMap.get(stage) / totalTime);
          stageNumLinesMap.put(stage, numLines);
          // startLine += numLines;
        }

        BufferedReader br = new BufferedReader(new FileReader(fileToRead));
        try {
          br.readLine(); br.readLine(); // header lines
        } catch(Exception e) {
          e.printStackTrace();
        }
         
        for(String stage: stageTimeMap.keySet()) {
          qstmt3.clearParameters();
          qstmt4.clearParameters();
          qstmt5.clearParameters();
          qstmt6.clearParameters();
          qstmt7.clearParameters();

          qstmt3.setObject(1, APP_ID); istmt1.setObject(1, APP_ID); istmt2.setObject(1, APP_ID);
          qstmt4.setObject(1, APP_ID); qstmt5.setObject(1, APP_ID); qstmt6.setObject(1, APP_ID); qstmt7.setObject(1, APP_ID);
          istmt3.setObject(1, APP_ID); istmt4.setObject(1, APP_ID);
          qstmt3.setObject(2, stage); istmt1.setObject(2, stage); istmt2.setObject(2, stage);
          qstmt4.setObject(2, stage); qstmt5.setObject(2, stage); qstmt6.setObject(2, stage); qstmt7.setObject(2, stage);
          istmt3.setObject(2, stage); istmt4.setObject(2, stage);
          qstmt3.setObject(3, exec); istmt1.setObject(3, exec); istmt2.setObject(3, exec);
          qstmt4.setObject(3, exec); qstmt5.setObject(3, exec); qstmt6.setObject(3, exec); qstmt7.setObject(3, exec);
          istmt3.setObject(3, exec); istmt4.setObject(3, exec);

          ResultSet rs3 = qstmt3.executeQuery();
          ResultSet rs4 = qstmt4.executeQuery();
          ResultSet rs5 = qstmt5.executeQuery();
          ResultSet rs6 = qstmt6.executeQuery();
          ResultSet rs7 = qstmt7.executeQuery();

          while(rs3.next()) {
            String host = rs3.getString("one");
            istmt1.setObject(4, host);
            Double ipBytesRead = rs3.getDouble("two");
            istmt1.setObject(5, ipBytesRead);
            Double ipRecordsRead = rs3.getDouble("three");
            istmt1.setObject(6, ipRecordsRead);
            Double shRemoteBytesRead = rs3.getDouble("four");
            istmt1.setObject(7, shRemoteBytesRead);
            Double shLocalBytesRead = rs3.getDouble("five");
            istmt1.setObject(8, shLocalBytesRead);
            Double shRecordsRead = rs3.getDouble("six");
            istmt1.setObject(9, shRecordsRead);
            Double opBytesWritten = rs3.getDouble("seven");
            istmt1.setObject(10, opBytesWritten);
            Double opRecordsWritten = rs3.getDouble("eight");
            istmt1.setObject(11, opRecordsWritten);
            Double shBytesWritten = rs3.getDouble("nine");
            istmt1.setObject(12, shBytesWritten);
            Double shRecordsWritten = rs3.getDouble("ten");
            istmt1.setObject(13, shRecordsWritten);
            istmt1.addBatch();

            istmt2.setObject(4, host);
            Double avgTaskTime = rs3.getDouble("elevan");
            istmt2.setObject(5, avgTaskTime);
            Double maxTaskTime = rs3.getDouble("twelve");
            istmt2.setObject(6, maxTaskTime);
            Double avgDeserTime = rs3.getDouble("thirteen");
            istmt2.setObject(7, avgDeserTime);
            Double maxDeserTime = rs3.getDouble("fourteen");
            istmt2.setObject(8, maxDeserTime);
            Double avgResultTime = rs3.getDouble("fifteen");
            istmt2.setObject(9, avgResultTime);
            Double maxResultTime = rs3.getDouble("sixteen");
            istmt2.setObject(10, maxResultTime);
            Double avgGCTime = rs3.getDouble("seventeen");
            istmt2.setObject(11, avgGCTime);
            Double maxGCTime = rs3.getDouble("eighteen");
            istmt2.setObject(12, maxGCTime);
            Double shFetchWaitTime = rs3.getDouble("nineteen");
            istmt2.setObject(13, shFetchWaitTime);
            Double shWriteTime = rs3.getDouble("twenty");
            istmt2.setObject(14, shWriteTime);
            istmt2.addBatch();

            Double count = rs3.getDouble("twoone");
            istmt3.setObject(4, count);
            Double failed = 0d;
            if(rs4.next()) { failed = rs4.getDouble("count"); }
            istmt3.setObject(5, failed);
            Double numProcessLocal = 0d;
            if(rs5.next()) { numProcessLocal = rs5.getDouble("count"); }
            istmt3.setObject(6, numProcessLocal);
            Double numNodeLocal = 0d;
            if(rs6.next()) { numNodeLocal = rs6.getDouble("count"); }
            istmt3.setObject(7, numNodeLocal);
            Double numRackLocal = 0d;
            if(rs7.next()) { numRackLocal = rs7.getDouble("count"); }
            istmt3.setObject(8, numRackLocal);
            Double avgResultSize = rs3.getDouble("twotwo");
            istmt3.setObject(9, avgResultSize);
            Double maxResultSize = rs3.getDouble("twothree");
            istmt3.setObject(10, maxResultSize);
            Double avgSpillSize = rs3.getDouble("twofour");
            istmt3.setObject(11, avgSpillSize);
            Double maxSpillSize = rs3.getDouble("twofive");
            istmt3.setObject(12, maxSpillSize);
            istmt3.addBatch();

            istmt4.setObject(4, host);
            Double maxHeapUsed = 0d;
            Double maxRSSUsed = 0d;
            Double maxCPUUsed = 0d;
            Double maxOldGenUsed = 0d; 
            Double numOldGC = 0d; 
            Double numYoungGC = 0d;
            Double totalOldCollection = 0d;
            Double totalYoungCollection = 0d;
            Double maxStorageUsed = 0d; 
            Double maxExecutionUsed = 0d;            
            try {
              String line = br.readLine();
              Double prevOldGen = 0d; Double prevYoungGen = 0d;
              for(int i=0; i<stageNumLinesMap.get(stage) && line!=null; i++) {
                String[] tokens = line.split("\t"); //hardcoding tab separator and token semantics
                Double heapUsed = Double.parseDouble(tokens[6]);
                Double RSSUsed = Double.parseDouble(tokens[14]);
                Double CPUUsed = Double.parseDouble(tokens[15]);
                Double oldUsed = Double.parseDouble(tokens[5]);
                Double youngUsed = Double.parseDouble(tokens[3]) + Double.parseDouble(tokens[4]);
                Double storageUsed = Double.parseDouble(tokens[20]);
                Double executionUsed = Double.parseDouble(tokens[21]);
                if(heapUsed > maxHeapUsed) { maxHeapUsed = heapUsed; }
                if(RSSUsed > maxRSSUsed) { maxRSSUsed = RSSUsed; }
                if(CPUUsed > maxCPUUsed) { maxCPUUsed = CPUUsed; }
                if(oldUsed > maxOldGenUsed) { maxOldGenUsed = oldUsed; }
                if(oldUsed < prevOldGen) { 
                  numOldGC++;
                  totalOldCollection += (prevOldGen - oldUsed);
                }
                if(youngUsed < prevYoungGen) {
                  numYoungGC++;
                  totalYoungCollection += (prevYoungGen - youngUsed);
                }
                prevOldGen = oldUsed;
                prevYoungGen = youngUsed;
                if(storageUsed > maxStorageUsed) { maxStorageUsed = storageUsed; }
                if(executionUsed > maxExecutionUsed) { maxExecutionUsed = executionUsed; }
                line = br.readLine();
              }
            } catch(Exception e) {
              e.printStackTrace();
            }
            istmt4.setObject(5, maxHeapUsed);
            istmt4.setObject(6, maxRSSUsed);
            istmt4.setObject(7, maxCPUUsed);
            istmt4.setObject(8, maxOldGenUsed);
            istmt4.setObject(9, numOldGC);
            istmt4.setObject(10, numYoungGC);
            istmt4.setObject(11, totalOldCollection);
            istmt4.setObject(12, totalYoungCollection);
            istmt4.setObject(13, maxStorageUsed);
            istmt4.setObject(14, maxExecutionUsed);
            istmt4.setObject(15, stageTimeMap.get(stage));
            istmt4.setObject(16, stageNumLinesMap.get(stage));
            istmt4.addBatch();
          }
        }
        br.close();
      }

      // HACK: Populate fake env table entry
      istmt5.setObject(1, APP_ID);
      istmt5.setObject(2, "SortDF");
      istmt5.setObject(3, new java.sql.Timestamp(java.util.Calendar.getInstance().getTime().getTime()));
      istmt5.setObject(4, 5);
      istmt5.setObject(5, 4);
      istmt5.setObject(6, 4);
      istmt5.setObject(7, 1);
      istmt5.setObject(8, 10);
      istmt5.setObject(9, 0.6);
      istmt5.setObject(10, 1);
      istmt5.setObject(11, 2);
      istmt5.setObject(12, "java");
      istmt5.setObject(13, "parallel");
      istmt5.setObject(14, 2);
      istmt5.addBatch();

System.out.println("--Running " + istmt1);
System.out.println("--Running " + istmt2);
System.out.println("--Running " + istmt3);
System.out.println("--Running " + istmt4);
System.out.println("--Running " + istmt5);
      istmt1.executeBatch();
      istmt2.executeBatch();
      istmt3.executeBatch();
      istmt4.executeBatch();
      istmt5.executeBatch();
      conn.commit();

      try { qstmt3.close(); } catch(Exception e) {}
      try { qstmt4.close(); } catch(Exception e) {}
      try { qstmt5.close(); } catch(Exception e) {}
      try { qstmt6.close(); } catch(Exception e) {}
      try { qstmt7.close(); } catch(Exception e) {}
      try { qstmt8.close(); } catch(Exception e) {}
      try { istmt1.close(); } catch(Exception e) {}
      try { istmt2.close(); } catch(Exception e) {}
      try { istmt3.close(); } catch(Exception e) {}
      try { istmt4.close(); } catch(Exception e) {}
      try { istmt5.close(); } catch(Exception e) {}
    } 
    catch(Exception e) {
          e.printStackTrace();
    } finally {
          try { conn.close(); } catch(Exception e) {}
    }
  }
}
