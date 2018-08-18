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
public class TrainExecutors
{

  // utility function to parse command line string showing flowgraph
  // e.g. "2->1#3->1,2#4->3"

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
    final String PERF_MONITORS_TABLE = "PERF_MONITORS";
    final String EXEC_TABLE = "EXEC_DATA";

    final String PERF_MONITOR_HOME = "/home/mayuresh/heap-logs/";
    final String PERF_FILE_PREFIX = "sparkOutput_worker_";

    final String APP_ID = args[0];
    final String APP_NAME = args[1];
    final Long maxHeap = Long.parseLong(args[2]);
    final Long yarnOverhead = Long.parseLong(args[3]);
    final Map<String, Set<String>> parents = parseParentsMap(args[4]);
    final Long totalStages = Long.parseLong(args[5]);

    try {
      Class.forName("com.mysql.jdbc.Driver").newInstance();
      conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);

      Statement stmt = conn.createStatement();

      String sql1 = "CREATE TABLE IF NOT EXISTS " + EXEC_TABLE + " (appId VARCHAR(255), " +
             "appName VARCHAR(255), stageId BIGINT, execId VARCHAR(8), ipBytes BIGINT, " +
             "cachedBytes BIGINT, shuffleBytesWritten BIGINT, " +
             "shLocalBytesRead BIGINT, shRemoteBytesRead BIGINT, opBytes BIGINT, " +
             "cacheBytesRead BIGINT, " + 
             "failed BOOLEAN, numTasks BIGINT, failedTasks BIGINT, " +
             "pLocalTasks BIGINT, diskBytesSpilled BIGINT, " + 
             "rssUsedAtStart BIGINT, rssUsedAtEnd BIGINT, " +
             "PRIMARY KEY(appId, stageId, execId))";
      stmt.executeUpdate(sql1);


      try { stmt.close(); } catch(Exception e) {}

      String qsql1 = "SELECT executorId, max(finishTime)-min(launchTime) as one, " +
             "max(stageId) as two from " +
             TASK_METRICS_TABLE + " where appId=\"" + APP_ID + "\"" +
             " group by executorId order by executorId";
      String qsql2 = "SELECT stageId, max(finishTime)-min(launchTime) as one from " +
             TASK_METRICS_TABLE + " where appId=\"" + APP_ID + "\"" +
             " and executorId = ? group by stageId order by stageId";
      String qsql3 = "SELECT count(1) as one, sum(ipBytesRead) as two, " + 
             "sum(shBytesWritten) as three, " +
             "sum(shLocalBytesRead) as four, sum(shRemoteBytesRead) as five, " +
             "sum(opBytesWritten) as six, sum(diskSpilled) as seven from " +
             TASK_METRICS_TABLE + " where appId=\"" + APP_ID + "\" " +
             " and taskId in (select taskId from " + TASK_METRICS_TABLE +
             " where appId=\"" + APP_ID + "\" " +
             "and stageId=? and executorId=? " +
             "group by taskId having count(1)=1)";
      String qsql4 = "select count(1) as cnt from " + TASK_METRICS_TABLE +
             " where appId=\"" + APP_ID + "\" " +
             "and taskId in (select taskId from " +
             TASK_METRICS_TABLE + " where appId=\"" + APP_ID +
             "\" and stageId = ? and executorId=? and failed = 1 " +
             "group by taskId having count(1)=1)";
      String qsql5 = "select count(1) as cnt, sum(ipBytesRead) as bytes " +
             " from " + TASK_METRICS_TABLE + " where appId=\"" + APP_ID + "\" " +
             "and taskId in (select taskId from " + TASK_METRICS_TABLE + 
             " where appId=\"" + APP_ID + "\" and stageId = ? and " +
             "executorId=? and locality = \'process_local\' " +
             "group by taskId having count(1)=1)";

      String isql1 = "INSERT INTO " + EXEC_TABLE + " values " +
             "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
      String usql1 = "UPDATE " + EXEC_TABLE + " SET cachedBytes = ? " + 
             "WHERE appId = \"" + APP_ID + "\" and stageId = ? and execId = ?";

      PreparedStatement qstmt1 = conn.prepareStatement(qsql1);
      PreparedStatement qstmt2 = conn.prepareStatement(qsql2);
      PreparedStatement qstmt3 = conn.prepareStatement(qsql3);
      PreparedStatement qstmt4 = conn.prepareStatement(qsql4);
      PreparedStatement qstmt5 = conn.prepareStatement(qsql5);
      PreparedStatement istmt1 = conn.prepareStatement(isql1);
      PreparedStatement ustmt1 = conn.prepareStatement(usql1);

      conn.setAutoCommit(false);

      // need the following maps to find bytes written by executors
      Map<String, Set<String>> execsMap = new LinkedHashMap<String, Set<String>>();
      Map<String, Double> cacheReadMap = new LinkedHashMap<String, Double>();
      Map<String, Double> tasksMap = new LinkedHashMap<String, Double>();

      ResultSet rs1 = qstmt1.executeQuery();
      while(rs1.next()) { // executors
        String exec = rs1.getString("executorId");
        Double totalTime = rs1.getDouble("one");
        Long maxStage = rs1.getLong("two");
        // hard-coding file path pattern
        String fileToRead = PERF_MONITOR_HOME + APP_ID + "/" + exec + "/" + PERF_FILE_PREFIX + APP_ID + "_" + exec + ".txt";
        Long totalLines = Files.lines(Paths.get(fileToRead)).count() - 2; //first two header lines
        BufferedReader br = new BufferedReader(new FileReader(fileToRead));
        try {
          br.readLine(); br.readLine(); // header lines
        } catch(Exception e) {
          e.printStackTrace();
        }
        Double currentRSS = 0.0;

        qstmt2.clearParameters();
        qstmt2.setObject(1, exec);
        ResultSet rs2 = qstmt2.executeQuery();
        while(rs2.next()) { // stages
          String stage = rs2.getString("stageId");
          Double sTime = rs2.getDouble("one");

          if(!execsMap.containsKey(stage)) {
            execsMap.put(stage, new LinkedHashSet<String>());
          }
          execsMap.get(stage).add(exec);
System.out.println("--Exploring: " + exec + ", stage: " + stage);

          qstmt3.clearParameters();
          qstmt3.setObject(1, stage);
          qstmt3.setObject(2, exec);
          qstmt4.clearParameters();
          qstmt4.setObject(1, stage);
          qstmt4.setObject(2, exec);
          qstmt5.clearParameters();
          qstmt5.setObject(1, stage);
          qstmt5.setObject(2, exec);

          istmt1.clearParameters();
          istmt1.setObject(1, APP_ID);
          istmt1.setObject(2, APP_NAME);
          istmt1.setObject(3, stage);
          istmt1.setObject(4, exec);

          // dataflow stats
          ResultSet rs3 = qstmt3.executeQuery();
          Double totalIPBytes = 0.0;
          Double nTasks = 0.0;
          while(rs3.next()) {
            totalIPBytes = rs3.getDouble("two");
            istmt1.setObject(6, 0.0);
            istmt1.setObject(7, rs3.getDouble("three"));
            istmt1.setObject(8, rs3.getDouble("four"));
            istmt1.setObject(9, rs3.getDouble("five"));
            istmt1.setObject(10, rs3.getDouble("six"));
            istmt1.setObject(16, rs3.getDouble("seven"));
            nTasks = rs3.getDouble("one");
          }

          // number stats
          Double fTasks = 0.0;
          Double pTasks = 0.0;
          Double localBytes = 0.0;
          ResultSet rs4 = qstmt4.executeQuery();
          while(rs4.next()) {
            fTasks = rs4.getDouble("cnt");
          }
          ResultSet rs5 = qstmt5.executeQuery();
          while(rs5.next()) {
            pTasks = rs5.getDouble("cnt");
            localBytes = rs5.getDouble("bytes");
          }
          istmt1.setObject(11, localBytes);
          cacheReadMap.put(stage+"#"+exec, localBytes);
System.out.println("Local bytes: " + localBytes + ", total bytes: " + totalIPBytes);
          tasksMap.put(stage+"#"+exec, nTasks);
          istmt1.setObject(5, totalIPBytes - localBytes); // disk bytes
          istmt1.setObject(13, nTasks);
          istmt1.setObject(14, fTasks);
          istmt1.setObject(15, pTasks);

          // metrics
          istmt1.setObject(17, currentRSS); //rssBegin
          Long linesToRead = Math.round(totalLines * sTime / totalTime);
System.out.println("Lines to read: " + linesToRead);
          try {
            for(long i=0; i<linesToRead; i++) {
              String line = br.readLine();
              String[] tokens = line.split("\t"); //hardcoding tab separator and token semantics
              currentRSS = Double.parseDouble(tokens[14]);
            }
          } catch(Exception e) {
            e.printStackTrace();
          }
System.out.println("Final RSS found: " + currentRSS);
          istmt1.setObject(18, currentRSS); //rssEnd
          // HACK: failed exec check
          if(stage.equals(maxStage.toString()) && maxStage < totalStages - 2) {
            istmt1.setObject(12, true); // failed
          } else {
            istmt1.setObject(12, false);
          }

          istmt1.addBatch();
        } // stages end

        br.close();

      } // executors end

System.out.println("--Running " + istmt1);
      istmt1.executeBatch();
      conn.commit();

      // update cached bytes
      Map<String, Double> cachedBytesMap = new HashMap<String, Double>();
      for(String stage: parents.keySet()) {
        for(String exec: execsMap.get(stage)) {
          Double readBytes = cacheReadMap.get(stage+"#"+exec);
          for(String source: parents.get(stage)) {
            String key = source+"#"+exec;
            if(cachedBytesMap.containsKey(key)) {
              readBytes = Math.max(0, readBytes - cachedBytesMap.get(key));
            } else {
              ustmt1.clearParameters();
              ustmt1.setObject(1, readBytes);
              ustmt1.setObject(2, source);
              ustmt1.setObject(3, exec);
              ustmt1.addBatch();
              cachedBytesMap.put(key, readBytes);
            }
          }
        }
      }
System.out.println("--Running " + ustmt1);
      ustmt1.executeBatch();
      conn.commit();

      try { qstmt1.close(); } catch(Exception e) {}
      try { qstmt2.close(); } catch(Exception e) {}
      try { qstmt3.close(); } catch(Exception e) {}
      try { qstmt4.close(); } catch(Exception e) {}
      try { qstmt5.close(); } catch(Exception e) {}
      try { istmt1.close(); } catch(Exception e) {}
      try { ustmt1.close(); } catch(Exception e) {}

    } 
    catch(Exception e) {
          e.printStackTrace();
    } finally {
          try { conn.close(); } catch(Exception e) {}
    }
  }
}
