package edu.duke.globallog.sparklogprocessor;

import com.github.wnameless.json.flattener.JsonFlattener;

import java.io.*;
import java.sql.*;
import java.util.Map;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

/**
 * Class to process a spark event log and flatten the task metrics
 * Writes output to mysql db
 * args[0] : spark event log file
 *
 */
public class FlattenTaskMetrics
{
    public static void main( String[] args )
    {
       // read input json file
       Configuration conf = new Configuration();
       String hadoopHome = System.getenv("HADOOP_HOME");
       conf.addResource(new Path(hadoopHome + "/etc/hadoop/core-site.xml"));
       conf.addResource(new Path(hadoopHome + "/etc/hadoop/hdfs-site.xml"));

       Path path = new Path(args[0]);
       FileSystem fs = null;
       FSDataInputStream inputStream = null;
       try {
         fs = path.getFileSystem(conf);
         inputStream = fs.open(path);
         System.out.println(inputStream.available());
       } catch(IOException e) {
         e.printStackTrace();
         System.exit(-1);
       }
       // fs.close();

       // extract application id
       String appID = path.getName();

       // connection to database
       Connection conn = null;
       Statement stmt = null;
       PreparedStatement istmt = null;

       final String DB_URL = "jdbc:mysql://localhost/test";
       final String DB_USER = "root";
       final String DB_PASSWORD = "database";
       final String TASK_TABLE = "TASK_METRICS";

       try {
          Class.forName("com.mysql.jdbc.Driver").newInstance();
          conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);

          stmt = conn.createStatement();
          String sql = "CREATE TABLE IF NOT EXISTS " + TASK_TABLE + " (appId VARCHAR(255), " +
             "stageId BIGINT, stageAttemptId BIGINT, taskId BIGINT, taskIndex BIGINT, " + 
             "taskAttempt BIGINT, launchTime BIGINT, executorId VARCHAR(8), host VARCHAR(255), " + 
             "locality VARCHAR(255), speculative BOOLEAN, resultTime INTEGER, finishTime BIGINT, " + 
             "failed BOOLEAN, deserializeTime BIGINT, runTime BIGINT, resultSize BIGINT, " + 
             "GCTime BIGINT, serializeTime BIGINT, memorySpilled BIGINT, diskSpilled BIGINT, " + 
             "bytesRead BIGINT, recordsRead BIGINT )";
          stmt.executeUpdate(sql);

          String isql = "INSERT INTO " + TASK_TABLE + " values " +
             "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
          istmt = conn.prepareStatement(isql);
          conn.setAutoCommit(false);

          BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
          String str = "";
          while((str = reader.readLine()) != null) {
            if(str.contains("SparkListenerTaskEnd")) {
              Map<String, Object> map = new JsonFlattener(str).flattenAsMap();
              istmt.setString(1, appID);
              istmt.setLong(2, (Long) map.get("Stage ID"));
              istmt.setLong(3, (Long) map.get("Stage Attempt ID"));
              istmt.setLong(4, (Long) map.get("Task Info.Task ID"));
              istmt.setLong(5, (Long) map.get("Task Info.Index"));
              istmt.setLong(6, (Long) map.get("Task Info.Attempt"));
              istmt.setLong(7, (Long) map.get("Task Info.Launch Time"));
              istmt.setString(8, (String) map.get("Task Info.Executor ID"));
              istmt.setString(9, (String) map.get("Task Info.Host"));
              istmt.setString(10, (String) map.get("Task Info.Locality"));
              istmt.setBoolean(11, (Boolean) map.get("Task Info.Speculative"));
              istmt.setLong(12, (Long) map.get("Task Info.Getting Result Time"));
              istmt.setLong(13, (Long) map.get("Task Info.Finish Time"));
              istmt.setBoolean(14, (Boolean) map.get("Task Info.Failed"));
              istmt.setLong(15, (Long) map.get("Task Metrics.Executor Deserialize Time"));
              istmt.setLong(16, (Long) map.get("Task Metrics.Executor Run Time"));
              istmt.setLong(17, (Long) map.get("Task Metrics.Result Size"));
              istmt.setLong(18, (Long) map.get("Task Metrics.JVM GC Time"));
              istmt.setLong(19, (Long) map.get("Task Metrics.Result Serialization Time"));
              istmt.setLong(20, (Long) map.get("Task Metrics.Memory Bytes Spilled"));
              istmt.setLong(21, (Long) map.get("Task Metrics.Disk Bytes Spilled"));
              if(map.get("Task Metrics.Input Metrics.Bytes Read") != null) {
                istmt.setLong(22, (Long) map.get("Task Metrics.Input Metrics.Bytes Read"));
                istmt.setLong(23, (Long) map.get("Task Metrics.Input Metrics.Records Read"));
              } else if(map.get("Task Metrics.Shuffle Read Metrics.Local Bytes Read") != null) {
                istmt.setLong(22, (Long) map.get("Task Metrics.Shuffle Read Metrics.Local Bytes Read") + (Long) map.get("Task Metrics.Shuffle Read Metrics.Remote Bytes Read"));
                istmt.setLong(23, (Long) map.get("Task Metrics.Shuffle Read Metrics.Total Records Read"));
              } else {
                istmt.setLong(22, 0L);
                istmt.setLong(23, 0L);
              }
              istmt.addBatch();
            }
          }
          int[] result = istmt.executeBatch();
          System.out.println("The number of rows inserted: "+ result.length);
          conn.commit();
       } catch(Exception e) {
          e.printStackTrace();
       } finally {
          try { stmt.close(); } catch(Exception e) {}
          try { istmt.close(); } catch(Exception e) {}
          try { conn.close(); } catch(Exception e) {}
          try { inputStream.close(); } catch(Exception e) {}
          try { fs.close(); } catch(Exception e) {}
       }
    }
}
