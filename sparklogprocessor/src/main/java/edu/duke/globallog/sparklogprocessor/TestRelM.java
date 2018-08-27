package edu.duke.globallog.sparklogprocessor;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import mikera.vectorz.Vector;

class Stage implements Serializable
{
  Long ipBytes;
  Long cachedBytes;
  Long shuffleBytesRead; 
  Long shuffleBytesWritten;
  Long opBytes;
  Long cacheStorage;
  Vector vec;
  int clusterId;

  Stage(Long a, Long b, Long c, Long d, Long e, Long f) {
    ipBytes = a;
    cachedBytes = b;
    shuffleBytesRead = c;
    shuffleBytesWritten = d;
    opBytes = e;
    cacheStorage = f;
    vec = Vector.of(new double[]{
      a.doubleValue(), b.doubleValue(), c.doubleValue(), d.doubleValue(), e.doubleValue(), f.doubleValue()});
    clusterId = -1;
  }

  void setClusterId(int n) {
    clusterId = n;
  }

  int getClusterId() {
    return clusterId;
  }

  //implementation of stage matching
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Stage)) {
       return false;
    }
    Stage s = (Stage) o;

    if(this.clusterId > -1 || s.clusterId > -1) {
      if(this.clusterId == s.clusterId) {
        return true;
      } else {
        return false;
      }
    }

    // use vector algebra
System.out.println("--Distance found in " + this.vec + " and " + s.vec + " -> regular: "
 + this.vec.toNormal().distance(s.vec.toNormal()) + " normalized: " + this.vec.distance(s.vec));
    if(normalDistance(s) <= 0.1) {
      return true;
    }
/*
    if(0.5*ipBytes <= s.ipBytes && 2*ipBytes >= s.ipBytes &&
      0.5*cachedBytes <= s.cachedBytes && 2*cachedBytes >= s.cachedBytes &&
      0.5*shuffleBytesRead <= s.shuffleBytesRead && 2*shuffleBytesRead >= s.shuffleBytesRead &&
      0.5*shuffleBytesWritten <= s.shuffleBytesWritten && 2*shuffleBytesWritten >= s.shuffleBytesWritten &&
      0.5*opBytes <= s.opBytes && 2*opBytes >= s.opBytes) {
        return true;
    }
*/
    return false;
  }

  // Find vector distance
  public double distance(Stage s) {
    return this.vec.distance(s.vec);
  }

  private double normalDistance(Stage s) {
    return this.vec.toNormal().distance(s.vec.toNormal());
  }

  @Override
  public int hashCode() {
    return "".hashCode();
  }

  @Override
  public String toString() {
    return vec.toString();
  }
}

class Config implements Serializable
{
  String appId;
  Long maxHeap;
  Long maxCores;
  Long yarnOverhead;
  Long numExecs;
  Double sparkMemoryFraction;
  Boolean offHeap;
  Long offHeapSize;
  String serializer;
  String gcAlgo; 
  Long newRatio;

  Config(String app, Long a, Long b, Long c, Long d, Double e, 
    Boolean f, Long g, String h, String i, Long j) {
    appId = app;
    maxHeap = a;
    maxCores = b;
    yarnOverhead = c;
    numExecs = d;
    sparkMemoryFraction = e;
    offHeap = f;
    offHeapSize = g;
    serializer = h;
    gcAlgo = i;
    newRatio = j;
  }

  @Override
  public boolean equals(Object c) {
    if (!(c instanceof Config)) {
       return false;
    }
    Config o = (Config) c;
    if(appId.equals(o.appId) && maxHeap.equals(o.maxHeap) &&
      maxCores.equals(o.maxCores) &&
      yarnOverhead.equals(o.yarnOverhead) &&
      numExecs.equals(o.numExecs) &&
      sparkMemoryFraction.equals(o.sparkMemoryFraction) &&
      offHeap.equals(o.offHeap) &&
      offHeapSize.equals(o.offHeapSize) &&
      serializer.equals(o.serializer) &&
      gcAlgo.equals(o.gcAlgo) &&
      newRatio.equals(o.newRatio)) {
        return true;
    }
    return false;
  }

  // Hack for an experiment, don't use in general
  public boolean relaxedEquals(Object c) {
    if (!(c instanceof Config)) {
       return false;
    }
    Config o = (Config) c;
    if(maxHeap.equals(o.maxHeap) &&
      maxCores.equals(o.maxCores) &&
      yarnOverhead.equals(o.yarnOverhead) &&
      numExecs.equals(o.numExecs) &&
      sparkMemoryFraction.equals(o.sparkMemoryFraction) &&
      newRatio.equals(o.newRatio)) {
        return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return appId.hashCode();
  }

  @Override
  public String toString() {
    return "[" + appId + ", " + maxHeap + ", " + maxCores + ", " +
      sparkMemoryFraction + ", " + newRatio + "]";
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public String getAppId() {
    return appId;
  }

  public Config clone() {
    return new Config(appId, maxHeap, maxCores, yarnOverhead, numExecs,
      sparkMemoryFraction, offHeap, offHeapSize, serializer, gcAlgo, newRatio);
  }
}

class Metrics implements Serializable
{
  Double failedExecs;
  Double failedTasks;
  Double maxStorage;
  Double maxExecution; 
  Double totalTime;
  Double maxUsedHeap;
  Double minUsageGap;
  Double maxOldGenUsed;
  Double totalGCTime;
  Double totalNumYoungGC;
  Double totalNumOldGC;
  Integer multiples;

  Metrics() {
    failedExecs = 0d;
    failedTasks = 0d;
    maxStorage = 0d;
    maxExecution = 0d;
    totalTime = 0d;
    maxUsedHeap = 0d;
    minUsageGap = 0d;
    maxOldGenUsed = 0d;
    totalGCTime = 0d;
    totalNumYoungGC = 0d;
    totalNumOldGC = 0d;
    multiples = 0;
  }

  Metrics(Double a, Double b, Double c, Double d, Double e, 
      Double f, Double g, Double h, Double i, Double j, Double k) {
    failedExecs = a;
    failedTasks = b;
    maxStorage = c;
    maxExecution = d;
    totalTime = e;
    maxUsedHeap = f;
    minUsageGap = g;
    maxOldGenUsed = h;
    totalGCTime = i;
    totalNumYoungGC = j;
    totalNumOldGC = k;
    multiples = 1;
  }

  Metrics add(Metrics m) {
    failedExecs += m.failedExecs;
    failedTasks += m.failedTasks;
    maxStorage += m.maxStorage;
    maxExecution += m.maxExecution;
    totalTime += m.totalTime;
    maxUsedHeap += m.maxUsedHeap;
    minUsageGap += m.minUsageGap;
    maxOldGenUsed += m.maxOldGenUsed;
    totalGCTime += m.totalGCTime;
    totalNumYoungGC += m.totalNumYoungGC;
    totalNumOldGC += m.totalNumOldGC;
    multiples += m.multiples;
    return this;
  }

  Metrics avg() {
    return new Metrics(failedExecs/multiples, failedTasks/multiples,
      maxStorage/multiples, maxExecution/multiples,
      totalTime/multiples, maxUsedHeap/multiples,
      minUsageGap/multiples, maxOldGenUsed/multiples, totalGCTime/multiples,
      totalNumYoungGC/multiples, totalNumOldGC/multiples);
  }

  @Override
  public String toString() {
    return  failedExecs + "\t" + failedTasks +
      "\t" + maxStorage + "\t" + maxExecution +
      "\t" + totalTime + "\t" + maxUsedHeap +
      "\t" + minUsageGap + "\t" + maxOldGenUsed +
      "\t" + totalGCTime + "\t" + totalNumYoungGC +
      "\t" + totalNumOldGC;
  }
}

class ConfigPlusMetrics implements Serializable {
  Config config;
  Metrics metrics;

  ConfigPlusMetrics(Config c, Metrics m) {
    config = c;
    metrics = m;
  }

  public Config getConfig() {
    return config;
  }

  public Metrics getMetrics() {
    return metrics;
  }
}

class EvalResult implements Serializable {

  Integer seqId; // sequence no
  String id; // title of eval record
  Integer numRecords; // that are clustered
  Integer numClusters;
  Double randIndex;
  Double condEntropy;
  Double normMutualInfo;

  public EvalResult(int a, String b, int c, int d, double e, double f, double g) {
    seqId = a;
    id = b;
    numRecords = c;
    numClusters = d;
    randIndex = e;
    condEntropy = f;
    normMutualInfo = g;
  }

  @Override
  public String toString() {
    return seqId + "\t" + id + "\t" + numRecords + "\t" + numClusters + "\t" +
      randIndex + "\t" + condEntropy + "\t" + normMutualInfo;
  }
}

/**
 * Class to combine stats from spark event logs and resource monitors and summarize them
 * Writes output to mysql db
 *
 */
public class TestRelM
{

  public static enum Mode { STAGE, CONFIG, TEST, SINGLE };

  public static int STAGE_CLUSTERS = 30;

  static List<String> IGNORE_APPS = new ArrayList<String>();
  static Map<String, List<Long>> IGNORE_STAGES = new LinkedHashMap<String, List<Long>>();  

  public static void main(String[] args) {
    // connection to database
    Connection conn = null;

    final String DB_URL = "jdbc:mysql://localhost/test";
    final String DB_USER = "root";
    final String DB_PASSWORD = "database";

    final String RELM_TABLE = "RELM_DATA";
    final String TEST_RELM_TABLE = "TEST_RELM";

 //   Config conf = new Config("", maxHeap, 2L, yarnOverhead, 10, 0.6, false, 
 //     maxHeap, "java", "parallel", 2L);

    Mode mode = Mode.STAGE;
    String resultsPath = "results-similar"; // default
    List<EvalResult> clusterResults = new ArrayList<EvalResult>();
    String clusterResultsFile = "results.tsv"; // default

    if("stage".equals(args[0])) {
      mode = Mode.STAGE;
      if(!("".equals(args[1]))) {
        resultsPath = args[1];
      }
    }

    if("config".equals(args[0])) {
      mode = Mode.CONFIG;
      if(!("".equals(args[1]))) {
        resultsPath = args[1];
      }
    }

    if("test".equals(args[0])) {
      mode = Mode.TEST;
      if(!("".equals(args[1]))) {
        resultsPath = args[1];
      }
    }

    if("single".equals(args[0])) {
      mode = Mode.SINGLE;
    }

    if(args.length > 2) {
      STAGE_CLUSTERS = Integer.parseInt(args[2]);
    }

    Stage[] stages = new Stage[6];
    stages[0] = new Stage(132744302L, 134217728L, 0L, 0L, 0L, 0L);
    //stages[1] = new Stage(134619472L, 0L, 0L, 0L, 0L, 36507221988L);
    stages[1] = new Stage(137818123L, 0L, 0L, 41033290L, 0L, 36507221988L);
    stages[2] = new Stage(0L, 0L, 57856938L, 33035663L, 0L, 35157221988L);
    //stages[4] = new Stage(0L, 0L, 11800L, 0L, 0L, 36507221988L);
    stages[3] = new Stage(132744302L, 134217728L, 0L, 0L, 0L, 36007221988L);
    //stages[6] = new Stage(134619472L, 0L, 0L, 0L, 0L, 36507221988L);
    stages[4] = new Stage(137818123L, 0L, 0L, 41033290L, 0L, 36507221988L);
    stages[5] = new Stage(0L, 0L, 57856938L, 33035663L, 0L, 35157221988L);
    //stages[9] = new Stage(0L, 0L, 11800L, 0L, 0L, 36507221988L);

    try {
      Class.forName("com.mysql.jdbc.Driver").newInstance();
      conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);

      Statement stmt = conn.createStatement();

      String sql1 = "DROP TABLE IF EXISTS " + TEST_RELM_TABLE;
      stmt.executeUpdate(sql1);

      String sql2 = "CREATE TABLE IF NOT EXISTS " + TEST_RELM_TABLE + " (appId VARCHAR(255), " +
             "maxHeap BIGINT, maxCores BIGINT, yarnOverhead BIGINT, numExecs BIGINT, " + 
             "sparkMemoryFraction DECIMAL(4,2), offHeap BOOLEAN, offHeapSize BIGINT, " +
             "serializer VARCHAR(255), gcAlgo VARCHAR(255), newRatio BIGINT, " +
             "failedExecs BIGINT, maxStorage BIGINT, maxExecution BIGINT, totalTime BIGINT, " +
             "maxUsedHeap BIGINT, minUsageGap BIGINT, maxOldGenUsed BIGINT, " +
             "totalGCTime BIGINT, totalNumYoungGC BIGINT, totalNumOldGC BIGINT, " +
             "PRIMARY KEY(appId))";
      stmt.executeUpdate(sql2);

      String qsql1 = "SELECT appId as one, ipBytes as two, cachedBytes as three, " +
             "shuffleBytesRead as four, shuffleBytesWritten as five, " +
             "opBytes as six, cacheStorage as seven, stageId as eight, " +
             "maxHeap, maxCores, yarnOverhead, numExecs, sparkMemoryFraction, " +
             "offHeap, offHeapSize, serializer, gcAlgo, newRatio, failedExecs, " +
             "failedTasks, maxStorage, maxExecution, totalTime, maxUsedHeap, minUsageGap, " +
             "maxOldGenUsed, totalGCTime, totalNumYoungGC, totalNumOldGC FROM " +
             RELM_TABLE;

      // ignore list
      String qsql2 = "SELECT appId from (SELECT appId, count(Distinct stageId) AS cnt FROM TASK_NUMBERS GROUP BY appId) AS s WHERE s.cnt<=1";

      // ignore stages
      String qsql3 = "SELECT appId, stageId FROM (SELECT appId, stageId, (max(finishTime) - min(launchTime)) AS rTime FROM TASK_METRICS_ALL GROUP BY appId, stageId) as s WHERE rTime < 1000";

      String isql1 = "INSERT INTO " + TEST_RELM_TABLE + " values " +
             "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

      PreparedStatement qstmt1 = conn.prepareStatement(qsql1);
      PreparedStatement qstmt2 = conn.prepareStatement(qsql2);
      PreparedStatement qstmt3 = conn.prepareStatement(qsql3);
      PreparedStatement istmt1 = conn.prepareStatement(isql1);
      conn.setAutoCommit(false);

      // populate ignore list
      ResultSet rs2 = qstmt2.executeQuery();
      while(rs2.next()) {
        IGNORE_APPS.add(rs2.getString("appId"));
      }
      rs2.close();

      // populate ignore stages
      ResultSet rs3 = qstmt3.executeQuery();
      while(rs3.next()) {
        String appId = rs3.getString("appId");
        Long stageId = rs3.getLong("stageId");
        if(IGNORE_STAGES.containsKey(appId)) {
          IGNORE_STAGES.get(appId).add(stageId);
        } else {
          List<Long> sList = new ArrayList<Long>();
          sList.add(stageId);
          IGNORE_STAGES.put(appId, sList); 
        }
      }
      rs3.close();

      // Scan stages in training data and build configMap
      ResultSet rs1 = qstmt1.executeQuery();

    if(mode.equals(Mode.SINGLE)) {
      Map<Config, Map<Stage, Metrics>> configMap = new LinkedHashMap<Config, Map<Stage, Metrics>>();

      for(Stage stage: stages) {
        rs1.beforeFirst();
        while(rs1.next()) {
          String appId = rs1.getString("one");
          if(ignoreApp(appId)) { continue; } // to be ignored
          Long stageId = rs1.getLong("eight");
          if(ignoreStage(appId, stageId)) { continue; }

          // find matching rows, store configs along with row ids
          Stage candidate = new Stage(rs1.getLong("two"), rs1.getLong("three"), 
            rs1.getLong("four"), rs1.getLong("five"), rs1.getLong("six"), rs1.getLong("seven"));
          if(stage.equals(candidate)) {
            Long maxHeapC = rs1.getLong("maxHeap");
            Config conf = new Config(appId, maxHeapC, 
              rs1.getLong("maxCores"), rs1.getLong("yarnOverhead"), rs1.getLong("numExecs"), 
              rs1.getDouble("sparkMemoryFraction"),
              rs1.getBoolean("offHeap"), rs1.getLong("offHeapSize"),
              rs1.getString("serializer"), rs1.getString("gcAlgo"), rs1.getLong("newRatio"));
            Metrics met = new Metrics((double)rs1.getLong("failedExecs"), 
              (double) rs1.getLong("failedTasks"), (double)rs1.getLong("maxStorage"),
              (double)rs1.getLong("maxExecution"), (double)rs1.getLong("totalTime"), ((double)rs1.getLong("maxUsedHeap"))/maxHeapC,
              (double)rs1.getLong("minUsageGap"), (double)rs1.getLong("totalGCTime"), (double)rs1.getLong("maxOldGenUsed"),
              (double)rs1.getLong("totalNumYoungGC"), (double)rs1.getLong("totalNumOldGC"));

            if(configMap.containsKey(conf)) {
              Map<Stage, Metrics> metricsMap = configMap.get(conf);
              if(metricsMap.containsKey(stage)) {
                metricsMap.get(stage).add(met);
              } else {
                metricsMap.put(stage, met);
              }
            } else {
              Map<Stage, Metrics> metricsMap = new LinkedHashMap<Stage, Metrics>();
              metricsMap.put(stage, met);
              configMap.put(conf, metricsMap);
            }
          }
        }
      }

      // parse configMap and write to test table
      for(Config conf: configMap.keySet()) {
        Map<Stage, Metrics> metricsMap = configMap.get(conf);
        if(metricsMap == null || metricsMap.isEmpty() || (metricsMap.size() % stages.length) != 0) {
          continue;
        }
        
        istmt1.clearParameters();
        istmt1.setObject(1, conf.appId);
        istmt1.setObject(2, conf.maxHeap);
        istmt1.setObject(3, conf.maxCores);
        istmt1.setObject(4, conf.yarnOverhead);
        istmt1.setObject(5, conf.numExecs);
        istmt1.setObject(6, conf.sparkMemoryFraction);
        istmt1.setObject(7, conf.offHeap);
        istmt1.setObject(8, conf.offHeapSize);
        istmt1.setObject(9, conf.serializer);
        istmt1.setObject(10, conf.gcAlgo);
        istmt1.setObject(11, conf.newRatio);

        Double failedExecs = 0d;
        Double failedTasks = 0d;
        Double maxStorage = 0d;
        Double maxExecution = 0d;
        Double totalTime = 0d;
        Double maxUsedHeap = 0d;
        Double minUsageGap = Double.MAX_VALUE;
        Double maxOldGenUsed = 0d;
        Double totalGCTime = 0d;
        Double totalNumYoungGC = 0d;
        Double totalNumOldGC = 0d;

        for(Stage stage: metricsMap.keySet()) {
          Metrics met = metricsMap.get(stage).avg();
          failedExecs += met.failedExecs;
          failedTasks += met.failedTasks;
          maxStorage = Math.max(maxStorage, met.maxStorage);
          maxExecution = Math.max(maxExecution, met.maxExecution);
          totalTime += met.totalTime;
          maxUsedHeap = Math.max(maxUsedHeap, met.maxUsedHeap);
          minUsageGap = Math.min(minUsageGap, met.minUsageGap);
          maxOldGenUsed = Math.max(maxOldGenUsed, met.maxOldGenUsed);
          totalGCTime += met.totalGCTime;
          totalNumYoungGC += met.totalNumYoungGC;
          totalNumOldGC += met.totalNumOldGC;
        }

        istmt1.setObject(12, failedExecs);
        istmt1.setObject(13, maxStorage);
        istmt1.setObject(14, maxExecution);
        istmt1.setObject(15, totalTime);
        istmt1.setObject(16, maxUsedHeap);
        istmt1.setObject(17, minUsageGap);
        istmt1.setObject(18, maxOldGenUsed);
        istmt1.setObject(19, totalGCTime);
        istmt1.setObject(20, totalNumYoungGC);
        istmt1.setObject(21, totalNumOldGC);
        istmt1.addBatch();
      }

System.out.println("--Running " + istmt1);
      istmt1.executeBatch();
      conn.commit();

    } else {
      // cluster stages first
      List<Stage> stageList = new ArrayList<Stage>();
      while(rs1.next()) {
        String appId = rs1.getString("one");
        if(ignoreApp(appId)) { continue; }; // to be ignored
        Long stageId = rs1.getLong("eight");
        if(ignoreStage(appId, stageId)) { continue; }

        Stage stage = new Stage(rs1.getLong("two"), rs1.getLong("three"),
             rs1.getLong("four"), rs1.getLong("five"), rs1.getLong("six"), rs1.getLong("seven"));
        stageList.add(stage);
      }
      DataClusterer stageClusterer = new DataClusterer();
      stageClusterer.setStages(stageList);
      stageClusterer.cluster(STAGE_CLUSTERS); // HACK: hard-coding number of clusters
      int[] stageClasses = stageClusterer.getAnswers();
      Stage[] stageCentroids = stageClusterer.getCentroids();
      int cnt = 0;
      for(Stage stage: stageList) {
        stage.setClusterId(stageClasses[cnt++]);
      }
      // set stage centroids
      cnt = 0;
      for(Stage cent: stageCentroids) {
        cent.setClusterId(cnt++);
      }

      // done clustering, reset resultset
      rs1.beforeFirst();
    if(mode.equals(Mode.STAGE)) {
     // SIMILAR mode starts
      Map<Stage, Map<Config, List<ConfigPlusMetrics>>> stageMap = new LinkedHashMap<Stage, Map<Config, List<ConfigPlusMetrics>>>();

      cnt = 0;
      BufferedWriter writer = new BufferedWriter(new FileWriter(
          new File(resultsPath + "/centroids.tsv")));
      writer.write("skyId\tClusterId\tStageId\n");
      while(rs1.next()) {
        String appId = rs1.getString("one");
        if(ignoreApp(appId)) { continue; }; // to be ignored
        Long stageId = rs1.getLong("eight");
        if(ignoreStage(appId, stageId)) { continue; }

        // find matching rows, store configs along with row ids
        Stage candidate = stageCentroids[stageList.get(cnt++).getClusterId()];//new Stage(rs1.getLong("two"), rs1.getLong("three"),
             //rs1.getLong("four"), rs1.getLong("five"), rs1.getLong("six"));
        Long maxHeapC = rs1.getLong("maxHeap");
        Config conf = new Config(appId, maxHeapC,
              rs1.getLong("maxCores"), rs1.getLong("yarnOverhead"), rs1.getLong("numExecs"),
              rs1.getDouble("sparkMemoryFraction"),
              rs1.getBoolean("offHeap"), rs1.getLong("offHeapSize"),
              rs1.getString("serializer"), rs1.getString("gcAlgo"), rs1.getLong("newRatio"));
        Config confExceptApp = conf.clone();
        confExceptApp.setAppId(""); 
        Metrics met = new Metrics((double)rs1.getLong("failedExecs"), 
              (double) rs1.getLong("failedTasks"), (double)rs1.getLong("maxStorage"),
              (double)rs1.getLong("maxExecution"), (double)rs1.getLong("totalTime"), ((double)rs1.getLong("maxUsedHeap"))/maxHeapC,
              (double)rs1.getLong("minUsageGap"), (double)rs1.getLong("totalGCTime"), (double)rs1.getLong("maxOldGenUsed"),
              (double)rs1.getLong("totalNumYoungGC"), (double)rs1.getLong("totalNumOldGC"));

        if(stageMap.containsKey(candidate)) {
          // check for existing config
          Map<Config, List<ConfigPlusMetrics>> confMap = stageMap.get(candidate);
          if(confMap.containsKey(confExceptApp)) {
            List<ConfigPlusMetrics> confList = confMap.get(confExceptApp);
            confList.add(new ConfigPlusMetrics(conf, met));
            confMap.put(confExceptApp, confList);
          } else {
            // new entry in confMap
            List<ConfigPlusMetrics> confList =
                new ArrayList<ConfigPlusMetrics>();
            confList.add(new ConfigPlusMetrics(conf, met));
            confMap.put(confExceptApp, confList);
          }
        } else {
          // new entry in map
          Map<Config, List<ConfigPlusMetrics>> confMap = 
              new LinkedHashMap<Config, List<ConfigPlusMetrics>>();
          List<ConfigPlusMetrics> confList = 
              new ArrayList<ConfigPlusMetrics>();
          confList.add(new ConfigPlusMetrics(conf, met));
          confMap.put(confExceptApp, confList);
          stageMap.put(candidate, confMap);
          writer.write(stageMap.size() + "\t" + candidate.getClusterId() + "\t" +
            candidate + System.getProperty("line.separator"));
        }
      }
      writer.close();

      // print skyline to csvs
      cnt = 1;
      Map<Stage, Set<Config>> skySet = new LinkedHashMap<Stage, Set<Config>>();
      Map<Stage, Map<Config, Metrics>> avgMetrics = new LinkedHashMap<Stage, Map<Config, Metrics>>();
      for(Stage stage: stageMap.keySet()) {
        // List for all metrics used in clustering
        List<Metrics> metList = new ArrayList<Metrics>();
        Map<Config, Metrics> avgMetMap = new LinkedHashMap<Config, Metrics>();
        List<Integer> confIds = new ArrayList<Integer>();

        writer = new BufferedWriter(new FileWriter(
          new File(resultsPath + "/" + cnt + ".tsv")));
        writer.write(stage + System.getProperty("line.separator"));

System.out.println("*Stats for stage " + stage.getClusterId() + " : " + stage);
        Map<Config, List<ConfigPlusMetrics>> confMap = stageMap.get(stage);
        int confId = 0;
        for(Config conf: confMap.keySet()) {
//System.out.println("**Conf map found: " + conf);
          List<ConfigPlusMetrics> confList = confMap.get(conf);
          Metrics aggMetrics = new Metrics();
          for(ConfigPlusMetrics confMet: confList) {
//System.out.println("***Metrics for conf " + confMet.getConfig() + ": " + confMet.getMetrics());
            writer.write(confMet.getConfig().getAppId() + "\t" + confMet.getMetrics() +
              System.getProperty("line.separator"));
            metList.add(confMet.getMetrics());
            confIds.add(confId);
            aggMetrics.add(confMet.getMetrics());
          }
          avgMetMap.put(conf, aggMetrics.avg());
          confId++;
        }
        writer.close();
        avgMetrics.put(stage, avgMetMap);
        // Cluster metrics for this stage, see if the clusters adhere to configs
//System.out.println("Clustering with config ids: " + Arrays.toString(confIds.toArray()));
        DataClusterer clusterer = new DataClusterer();
        clusterer.setMetrics(metList);
        clusterer.cluster(confMap.size());
        int[] result = clusterer.getAnswers();
        List<Integer> resultList = Arrays.stream(result).boxed().collect(Collectors.toList());
        // Evaluate cluster quality
        ClusterEval eval = new ClusterEval();
        eval.setClusterClasses(confIds);
        eval.setClusterIds(resultList);
        eval.pairEval();
System.out.println("Evaluation results: \n TP=" + eval.truePositives() + "\n FP=" + eval.falsePositives() + "\n TN=" + eval.trueNegatives() + "\n FN=" + eval.falseNegatives() + "\n Rand Index = " + eval.randIndex() + "\n Jaccard = " + eval.jaccard() + "\n Cond entropy = " + eval.conditionalEntropy() + "\n Relative cond entropy = " + eval.relativeCondEntropy() + "\n Norm mutual info = " + eval.normMutualInfo());
        clusterResults.add(new EvalResult(
          cnt, stage.toString(), resultList.size(), confMap.size(), 
          eval.randIndex(), eval.conditionalEntropy(), eval.normMutualInfo()));

        // Skyline for stage
        List<Metrics> skyMetList = avgMetMap.values().stream().collect(Collectors.toList());
        SortableMetrics sortMet = new SortableMetrics(skyMetList);
        Integer[] skyline = sortMet.skyline(0.1);
        Arrays.sort(skyline);
        Set<Config> skyConfigs = new HashSet<Config>();
        int nextSky = 0;
        int confCount = 0;
        for(Config cand: confMap.keySet()) {
          if(skyline[nextSky] == confCount) {
            skyConfigs.add(cand);
            nextSky++;
          }
          if(nextSky >= skyline.length) { break; }
          confCount++;
        }
        skySet.put(stage, skyConfigs);

        /*int nextSky = 0;
        int confCount = 0;
System.out.println("Skyline configs: \n");
        writer = new BufferedWriter(new FileWriter(
          new File(resultsPath + "/sky-" + cnt + ".tsv")));
        writer.write(stage + System.getProperty("line.separator"));

        List<ConfigPlusMetrics> skyList = new ArrayList<ConfigPlusMetrics>();
        for(Config conf: confMap.keySet()) {
          if(nextSky < skyline.length && skyline[nextSky] == confCount) {
            String confStr = conf.toString();
            writer.write(confStr + "\t" + skyMetList.get(confCount) +
              System.getProperty("line.separator"));
System.out.println("--" + confStr + "\t" + skyMetList.get(confCount));
            skyList.add(new ConfigPlusMetrics(conf, skyMetList.get(confCount)));
            nextSky++;
          }
          confCount++;
        }
        skyMap.put(stage, skyList);*/
        /*for(Config conf: confMap.keySet()) {
          int prevCount = confCount;
          List<ConfigPlusMetrics> confList = confMap.get(conf);
          confCount += confList.size();
          while(nextSky < skyline.length && skyline[nextSky] < confCount) {
            String appId = confList.get(skyline[nextSky] - prevCount).getConfig().toString();
            writer.write(appId + System.getProperty("line.separator"));
System.out.println("--" + appId + "\n");
            nextSky++;
          }
        }
        writer.close();*/

        cnt++;
      }

      // merge skylines from all stages
      Set<Config> mergedSkySet = skySet.values().stream()
        .flatMap(x -> x.stream())
        .collect(Collectors.toSet());

      // build skyMap
      Map<Stage, List<ConfigPlusMetrics>> skyMap = new LinkedHashMap<Stage, List<ConfigPlusMetrics>>();
      cnt = 1;
      for(Stage stage: avgMetrics.keySet()) {
        writer = new BufferedWriter(new FileWriter(
          new File(resultsPath + "/sky-" + cnt + ".tsv")));
        writer.write(stage + System.getProperty("line.separator"));
System.out.println("-Skyline for stage: " + stage);
        List<ConfigPlusMetrics> skyList = new ArrayList<ConfigPlusMetrics>();
        Map<Config, Metrics> confMets = avgMetrics.get(stage);
        for(Config conf: mergedSkySet) {
          if(confMets.containsKey(conf)) {
            skyList.add(new ConfigPlusMetrics(conf, confMets.get(conf)));
            if(skySet.get(stage).contains(conf)) {
System.out.println("Adding to skyline *legit* config: " + conf); 
              writer.write(conf.toString() + "\t" + confMets.get(conf) +
                System.getProperty("line.separator"));
            } else {
System.out.println("Adding to skyline *forced* config: " + conf);
            }
          }
        }
        skyMap.put(stage, skyList);
        writer.close();
        cnt++;
      }

      // write skyMap
      try(
        FileOutputStream fout = new FileOutputStream(resultsPath + "/sky-results.ser", true);
        ObjectOutputStream oos = new ObjectOutputStream(fout);
      ){
        oos.writeObject(skyMap);
      } catch (Exception ex) {
         ex.printStackTrace();
      }
      

    } else if(mode.equals(Mode.CONFIG)) {
      Map<Config, Map<Stage, List<ConfigPlusMetrics>>> confMap = 
          new LinkedHashMap<Config, Map<Stage, List<ConfigPlusMetrics>>>();

      cnt = 0;
      while(rs1.next()) {
        String appId = rs1.getString("one");
        if(ignoreApp(appId)) { continue; }; // to be ignored
        Long stageId = rs1.getLong("eight");
        if(ignoreStage(appId, stageId)) { continue; }

        Stage stage = stageList.get(cnt++);
        Long maxHeapC = rs1.getLong("maxHeap");
        Config conf = new Config(appId, maxHeapC,
              rs1.getLong("maxCores"), rs1.getLong("yarnOverhead"), rs1.getLong("numExecs"),
              rs1.getDouble("sparkMemoryFraction"),
              rs1.getBoolean("offHeap"), rs1.getLong("offHeapSize"),
              rs1.getString("serializer"), rs1.getString("gcAlgo"), rs1.getLong("newRatio"));
        Config confExceptApp = conf.clone();
        confExceptApp.setAppId(""); 
        Metrics met = new Metrics((double)rs1.getLong("failedExecs"),
              (double) rs1.getLong("failedTasks"), (double)rs1.getLong("maxStorage"),
              (double)rs1.getLong("maxExecution"), (double)rs1.getLong("totalTime"), ((double)rs1.getLong("maxUsedHeap"))/maxHeapC,
              (double)rs1.getLong("minUsageGap"), (double)rs1.getLong("totalGCTime"), (double)rs1.getLong("maxOldGenUsed"),
              (double)rs1.getLong("totalNumYoungGC"), (double)rs1.getLong("totalNumOldGC"));

        if(confMap.containsKey(confExceptApp)) {
          Map<Stage, List<ConfigPlusMetrics>> stageMap = confMap.get(confExceptApp);
          if(stageMap.containsKey(stage)) {
            List<ConfigPlusMetrics> metList = stageMap.get(stage);
            metList.add(new ConfigPlusMetrics(conf, met));
          } else {
            List<ConfigPlusMetrics> metList = new ArrayList<ConfigPlusMetrics>();
            metList.add(new ConfigPlusMetrics(conf, met));
            stageMap.put(stage, metList);
          }
        } else {
          List<ConfigPlusMetrics> metList = new ArrayList<ConfigPlusMetrics>();
          metList.add(new ConfigPlusMetrics(conf, met));
          Map<Stage, List<ConfigPlusMetrics>> stageMap = 
            new LinkedHashMap<Stage, List<ConfigPlusMetrics>>();
          stageMap.put(stage, metList);
          confMap.put(confExceptApp, stageMap);
        }
      }

      // cluster stages
      int confCount = 1;
      for(Config conf: confMap.keySet()) {
        List<Metrics> metList = new ArrayList<Metrics>();
        List<Integer> classIds = new ArrayList<Integer>();
        BufferedWriter writer = new BufferedWriter(new FileWriter(
          new File(resultsPath + "/" + confCount + ".tsv")));
        writer.write(conf + System.getProperty("line.separator"));

System.out.println("*Stats for conf " + confCount + " : " + conf);
        Map<Stage, List<ConfigPlusMetrics>> stageMap = confMap.get(conf);
        cnt = 0; 
        for(Stage stage: stageMap.keySet()) {
          for(ConfigPlusMetrics confMet: stageMap.get(stage)) {
            writer.write(confMet.getConfig().getAppId() + "\t" + confMet.getMetrics() +
              System.getProperty("line.separator"));
            metList.add(confMet.getMetrics());
            classIds.add(cnt);
          }
          cnt++;
        }

        DataClusterer clusterer = new DataClusterer();
        clusterer.setMetrics(metList);
        clusterer.cluster(stageMap.size());
        int[] result = clusterer.getAnswers();
        List<Integer> resultList = Arrays.stream(result).boxed().collect(Collectors.toList());
        // Evaluate cluster quality
        ClusterEval eval = new ClusterEval();
        eval.setClusterClasses(classIds);
        eval.setClusterIds(resultList);
        eval.pairEval();
System.out.println("Evaluation results: \n TP=" + eval.truePositives() + "\n FP=" + eval.falsePositives() + "\n TN=" + eval.trueNegatives() + "\n FN=" + eval.falseNegatives() + "\n Rand Index = " + eval.randIndex() + "\n Jaccard = " + eval.jaccard() + "\n Cond entropy = " + eval.conditionalEntropy() + "\n Relative cond entropy = " + eval.relativeCondEntropy() + "\n Norm mutual info = " + eval.normMutualInfo());
        clusterResults.add(new EvalResult(
          confCount, conf.toString(), resultList.size(), stageMap.size(),
          eval.randIndex(), eval.conditionalEntropy(), eval.normMutualInfo()));

        writer.close();
        confCount++;
      }
    
    } else if(mode.equals(Mode.TEST)) {
      Map<Stage, List<ConfigPlusMetrics>> skyMap = new LinkedHashMap<Stage, List<ConfigPlusMetrics>>();
      try (
        FileInputStream streamIn = new FileInputStream(resultsPath + "/sky-results.ser");
        ObjectInputStream ois = new ObjectInputStream(streamIn);
      ) {
        skyMap = (LinkedHashMap) ois.readObject();
      } catch (Exception e) {
        e.printStackTrace();
      }

      Map<Config, Metrics> skyResults = new LinkedHashMap<Config, Metrics>();
      boolean first = true; 
      for(Stage stage: stages) {
        Stage match = closestCentroid(stageCentroids, stage);
        List<ConfigPlusMetrics> candidates = skyMap.get(match);
        if(first) {
          for(ConfigPlusMetrics candidate: candidates) {
            skyResults.put(candidate.getConfig(), candidate.getMetrics());
          }
          first = false;
        } else {
          List<Config> toBeDeleted = new ArrayList<Config>();
          for(Config conf: skyResults.keySet()) {
            boolean found = false;
            for(ConfigPlusMetrics candidate: candidates) {
              if(candidate.getConfig().equals(conf)) {
System.out.println("Found config: " + conf);
                Metrics met = skyResults.get(conf);
                Metrics nmet = candidate.getMetrics();
                met.failedExecs += nmet.failedExecs;
                met.failedTasks += nmet.failedTasks;
                met.maxStorage = Math.max(met.maxStorage, nmet.maxStorage);
                met.maxExecution = Math.max(met.maxExecution, nmet.maxExecution);
                met.totalTime += nmet.totalTime;
                met.maxUsedHeap = Math.max(met.maxUsedHeap, nmet.maxUsedHeap);
                met.minUsageGap = Math.min(met.minUsageGap, nmet.minUsageGap);
                met.maxOldGenUsed = Math.max(met.maxOldGenUsed, nmet.maxOldGenUsed);
                met.totalGCTime += nmet.totalGCTime;
                met.totalNumYoungGC += nmet.totalNumYoungGC;
                met.totalNumOldGC += nmet.totalNumOldGC;
                found = true;
                break;
              }
            }
            if(!found) {
              toBeDeleted.add(conf);
            }
          }
          for(Config conf: toBeDeleted) {
System.out.println("Removing config: " + conf);
            skyResults.remove(conf);
          }
        }
      }

      // write skyresults
System.out.println("-- Possible candidates along with estimated metrics: " + skyResults);
       BufferedWriter writer = new BufferedWriter(new FileWriter(
          new File(resultsPath + "/candidates")));
       writer.write(skyResults + System.getProperty("line.separator"));
       writer.close();
    } 

/*
    else if(mode.equals(Mode.METRICS)) {

      List<Metrics> metList = new ArrayList<Metrics>();
      while(rs1.next()) {
        String appId = rs1.getString("one");
        if(ignoreApp(appId)) { continue; }; // to be ignored
        Long stageId = rs1.getLong("eight");
        if(ignoreStage(appId, stageId)) { continue; }

        Long maxHeapC = rs1.getLong("maxHeap");
        Metrics met = new Metrics((double)rs1.getLong("failedExecs"), (double)rs1.getLong("maxStorage"),
              (double)rs1.getLong("maxExecution"), (double)rs1.getLong("totalTime"), ((double)rs1.getLong("maxUsedHeap"))/maxHeapC,
              (double)rs1.getLong("minUsageGap"), (double)rs1.getLong("totalGCTime"), (double)rs1.getLong("maxOldGenUsed"),
              (double)rs1.getLong("totalNumYoungGC"), (double)rs1.getLong("totalNumOldGC"));

        metList.add(met);
      }
    
      List<Integer> classList = Arrays.stream(stageClasses).boxed().collect(Collectors.toList());
// random results
// List<Integer> resultList = new ArrayList<Integer>();
//      for(int i=0; i<stageClasses.length; i++) {
//        resultList.add(java.util.concurrent.ThreadLocalRandom.current().nextInt(0, STAGE_CLUSTERS));
//      }
//      int numClusters = java.util.Collections.max(classList) + 1;
      DataClusterer clusterer = new DataClusterer();
      clusterer.setMetrics(metList);
      clusterer.cluster(STAGE_CLUSTERS);
      int[] result = clusterer.getAnswers();
      List<Integer> resultList = Arrays.stream(result).boxed().collect(Collectors.toList());
      // Evaluate cluster quality
      ClusterEval eval = new ClusterEval();
      eval.setClusterClasses(classList);
      eval.setClusterIds(resultList);
      eval.pairEval();
System.out.println("Evaluation results: \n TP=" + eval.truePositives() + "\n FP=" + eval.falsePositives() + "\n TN=" + eval.trueNegatives() + "\n FN=" + eval.falseNegatives() + "\n Rand Index = " + eval.randIndex() + "\n Jaccard = " + eval.jaccard() + "\n Cond entropy = " + eval.conditionalEntropy() + "\n Relative cond entropy = " + eval.relativeCondEntropy() + "\n Norm mutual info = " + eval.normMutualInfo());
      clusterResults.add(new EvalResult(
          0, "all stages", resultList.size(), STAGE_CLUSTERS,
          eval.randIndex(), eval.conditionalEntropy(), eval.normMutualInfo()));

    }// if else done
    */  
      if(clusterResults.size() > 0) {
        BufferedWriter writer = new BufferedWriter(new FileWriter(
          new File(resultsPath + "/" + clusterResultsFile )));
        for(EvalResult result: clusterResults) {
          writer.write(result + System.getProperty("line.separator"));
        }
        writer.close();
      }

    }

      try { qstmt1.close(); } catch(Exception e) {}
      try { qstmt2.close(); } catch(Exception e) {}
      try { qstmt3.close(); } catch(Exception e) {}
      try { istmt1.close(); } catch(Exception e) {}
    } 
    catch(Exception e) {
          e.printStackTrace();
    } finally {
          try { conn.close(); } catch(Exception e) {}
    }
  }

  // Find a stage centroid closest to a candidate stage
  static Stage closestCentroid(Stage[] centroids, Stage lookFor) {
    int index = 0;
    double distance = Double.MAX_VALUE;
    for(int i=0; i<centroids.length; i++) {
      double d = lookFor.distance(centroids[i]);
      if(d < distance) {
        distance = d;
        index = i;
      }
    }
System.out.println("Stage " + lookFor + " is closest to " + centroids[index] + " with distance " + distance);
    return centroids[index];
  }

  //HACK: hardcoded
  static boolean ignoreApp(String appId) {
    // first if is covered by second if, REMOVE
    if(appId.compareTo("application_1508545462036_0175") >=0 && appId.compareTo("application_1508545462036_0190") <= 0 ) {
      return true; // these apps are failed but still in our database
    }
    if(IGNORE_APPS.contains(appId)) {
      return true;
    }
    return false;
  }

  static boolean ignoreStage(String appId, Long stageId) {
    if(IGNORE_STAGES.containsKey(appId)) {
      if(IGNORE_STAGES.get(appId).contains(stageId)) {
        return true;
      }
    }
    return false;
  }
}
