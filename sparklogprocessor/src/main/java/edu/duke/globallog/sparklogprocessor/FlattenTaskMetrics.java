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
       final String TASK_TABLE = "TASK_METRICS_ALL";

       try {
          Class.forName("com.mysql.jdbc.Driver").newInstance();
          conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);

          stmt = conn.createStatement();
          String sql = "CREATE TABLE IF NOT EXISTS " + TASK_TABLE + " (appId VARCHAR(255), " +
             "stageId BIGINT, stageAttemptId BIGINT, taskType VARCHAR(255),  taskId BIGINT, taskIndex BIGINT, " + 
             "taskAttempt BIGINT, launchTime BIGINT, executorId VARCHAR(8), host VARCHAR(255), " + 
             "locality VARCHAR(255), speculative BOOLEAN, resultTime BIGINT, finishTime BIGINT, " + 
             "failed BOOLEAN, deserializeTime BIGINT, runTime BIGINT, resultSize BIGINT, " + 
             "GCTime BIGINT, serializeTime BIGINT, memorySpilled BIGINT, diskSpilled BIGINT, " + 
             "ipBytesRead BIGINT, ipRecordsRead BIGINT, " +
             "shRemoteBytesRead BIGINT, shLocalBytesRead BIGINT, shRecordsRead BIGINT, shFetchWaitTime BIGINT, " +
             "opBytesWritten BIGINT, opRecordsWritten BIGINT, " +
             "shBytesWritten BIGINT, shRecordsWritten BIGINT, shWriteTime BIGINT )";
          stmt.executeUpdate(sql);

          String isql = "INSERT INTO " + TASK_TABLE + " values " +
             "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
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
              istmt.setString(4, (String) map.get("Task Type"));
              istmt.setLong(5, (Long) map.get("Task Info.Task ID"));
              istmt.setLong(6, (Long) map.get("Task Info.Index"));
              istmt.setLong(7, (Long) map.get("Task Info.Attempt"));
              istmt.setLong(8, (Long) map.get("Task Info.Launch Time"));
              istmt.setString(9, (String) map.get("Task Info.Executor ID"));
              istmt.setString(10, (String) map.get("Task Info.Host"));
              istmt.setString(11, (String) map.get("Task Info.Locality"));
              istmt.setBoolean(12, (Boolean) map.get("Task Info.Speculative"));
              istmt.setLong(13, (Long) map.get("Task Info.Getting Result Time"));
              istmt.setLong(14, (Long) map.get("Task Info.Finish Time"));
              istmt.setBoolean(15, (Boolean) map.get("Task Info.Failed"));
		istmt.setLong(16, 0L);
		istmt.setLong(17, 0L);
		istmt.setLong(18, 0L);
		istmt.setLong(19, 0L);
		istmt.setLong(20, 0L);
		istmt.setLong(21, 0L);
		istmt.setLong(22, 0L);
                istmt.setLong(23, 0L);
                istmt.setLong(24, 0L);
                istmt.setLong(25, 0L);
                istmt.setLong(26, 0L);
                istmt.setLong(27, 0L);
                istmt.setLong(28, 0L);
                istmt.setLong(29, 0L);
                istmt.setLong(30, 0L);
                istmt.setLong(31, 0L);
                istmt.setLong(32, 0L);
                istmt.setLong(33, 0L);
		
              try {istmt.setLong(16, (Long) map.get("Task Metrics.Executor Deserialize Time"));} catch(Exception e) {}
              try {istmt.setLong(17, (Long) map.get("Task Metrics.Executor Run Time"));} catch(Exception e) {}
              try {istmt.setLong(18, (Long) map.get("Task Metrics.Result Size"));} catch(Exception e) {}
              try {istmt.setLong(19, (Long) map.get("Task Metrics.JVM GC Time"));} catch(Exception e) {}
              try {istmt.setLong(20, (Long) map.get("Task Metrics.Result Serialization Time"));} catch(Exception e) {}
              try {istmt.setLong(21, (Long) map.get("Task Metrics.Memory Bytes Spilled"));} catch(Exception e) {}
              try {istmt.setLong(22, (Long) map.get("Task Metrics.Disk Bytes Spilled"));} catch(Exception e) {}

              if(map.get("Task Metrics.Input Metrics.Bytes Read") != null) {
                istmt.setLong(23, (Long) map.get("Task Metrics.Input Metrics.Bytes Read"));
                istmt.setLong(24, (Long) map.get("Task Metrics.Input Metrics.Records Read"));
              }
              if(map.get("Task Metrics.Shuffle Read Metrics.Local Bytes Read") != null) {
                istmt.setLong(25, (Long) map.get("Task Metrics.Shuffle Read Metrics.Remote Bytes Read"));
                istmt.setLong(26, (Long) map.get("Task Metrics.Shuffle Read Metrics.Local Bytes Read"));
                istmt.setLong(27, (Long) map.get("Task Metrics.Shuffle Read Metrics.Total Records Read"));
                istmt.setLong(28, (Long) map.get("Task Metrics.Shuffle Read Metrics.Fetch Wait Time"));
              } 
              if(map.get("Task Metrics.Output Metrics.Bytes Written") != null) {
                istmt.setLong(29, (Long) map.get("Task Metrics.Output Metrics.Bytes Written"));
                istmt.setLong(30, (Long) map.get("Task Metrics.Output Metrics.Records Written"));
              }
              if(map.get("Task Metrics.Shuffle Write Metrics.Shuffle Bytes Written") != null) {
                istmt.setLong(31, (Long) map.get("Task Metrics.Shuffle Write Metrics.Shuffle Bytes Written"));
                istmt.setLong(32, (Long) map.get("Task Metrics.Shuffle Write Metrics.Shuffle Records Written"));
                istmt.setLong(33, (Long) map.get("Task Metrics.Shuffle Write Metrics.Shuffle Write Time"));
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
