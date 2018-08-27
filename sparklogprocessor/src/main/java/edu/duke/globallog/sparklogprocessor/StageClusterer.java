package edu.duke.globallog.sparklogprocessor;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

class AppPlusStage {
    String appId;
    Long stageId;
    int clusterId;

    AppPlusStage(String a, Long b) {
      appId = a;
      stageId = b;
    }

    String getAppId() { return appId; }
    Long getStageId() { return stageId; }
    Integer getClusterId() { return clusterId; }
    void setClusterId(int n) { clusterId = n; }
}

public class StageClusterer extends TrainingBase {
  int STAGE_CLUSTERS = 30;
  String resultsPath = "cluster-results"; // default

  List<String> IGNORE_APPS = new ArrayList<String>();
  Map<String, List<Long>> IGNORE_STAGES = new LinkedHashMap<String, List<Long>>();  

  List<Stage> allStages = new ArrayList<Stage>();
  List<AppPlusStage> stageIdentifiers = new ArrayList<AppPlusStage>();

  StageClusterer(int n, String path) {
    STAGE_CLUSTERS = n;
    resultsPath = path;
    startConnection();
  }

  void cleanup() {
    commit();
    stopConnection();
  }

  void populateIgnoreApps() {
    String qsql = "SELECT appId from (SELECT appId, count(Distinct stageId) AS cnt FROM TASK_NUMBERS GROUP BY appId) AS s WHERE s.cnt<=1";
    try {
      Statement qstmt = newStatement();
      // populate ignore list
      ResultSet rs2 = qstmt.executeQuery(qsql);
      while(rs2.next()) {
        IGNORE_APPS.add(rs2.getString("appId"));
      }
      rs2.close();
      closeStatement(qstmt);
    } catch(Exception e) {
      e.printStackTrace();
    }
  }

  void populateIgnoreStages() {
    String qsql = "SELECT appId, stageId FROM (SELECT appId, stageId, (max(finishTime) - min(launchTime)) AS rTime FROM TASK_METRICS_ALL GROUP BY appId, stageId) as s WHERE rTime < 1000";
    try {
      Statement qstmt = newStatement();
      // populate ignore stages
      ResultSet rs3 = qstmt.executeQuery(qsql);
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
      closeStatement(qstmt);
    } catch(Exception e) {
      e.printStackTrace();
    }
  }

  void populateAllStages() {
    String qsql = "SELECT appId as one, ipBytes as two, cachedBytes as three, " +
           "shuffleBytesRead as four, shuffleBytesWritten as five, " +
           "opBytes as six, cacheBytesRead as seven, stageId as eight " +
           "FROM " + RELM_TABLE;
    try {
      Statement qstmt = newStatement();
      ResultSet rs1 = qstmt.executeQuery(qsql);
      while(rs1.next()) {
        String appId = rs1.getString("one");
        if(IGNORE_APPS.contains(appId)) { continue; }; // to be ignored
        Long stageId = rs1.getLong("eight");
        if(IGNORE_STAGES.containsKey(appId)
            && IGNORE_STAGES.get(appId).contains(stageId)) {
          continue; 
        }
        stageIdentifiers.add(new AppPlusStage(appId, stageId));
        Stage stage = new Stage(rs1.getLong("two"), rs1.getLong("three"),
             rs1.getLong("four"), rs1.getLong("five"), rs1.getLong("six"), rs1.getLong("seven"));
        allStages.add(stage);
      }
      rs1.close();
      closeStatement(qstmt);
    } catch(Exception e) {
      e.printStackTrace();
    }
  }

  void clusterStages() {
    DataClusterer stageClusterer = new DataClusterer();
    stageClusterer.setStages(allStages);
    stageClusterer.cluster(STAGE_CLUSTERS); // HACK: hard-coding number of clusters
    int[] stageClasses = stageClusterer.getAnswers();
    Stage[] stageCentroids = stageClusterer.getCentroids();

    int cnt = 0;
    for(AppPlusStage stage: stageIdentifiers) {
      stage.setClusterId(stageClasses[cnt++]);
    }
    stageIdentifiers.sort(
      (o1, o2) -> o1.getClusterId().compareTo(o2.getClusterId()));

    // write output
    int current = -1;
    BufferedWriter writer = null;
    try {
      for(AppPlusStage stage: stageIdentifiers) {
        int clusterId = stage.getClusterId();
        if(clusterId != current) {
          // start writing to new file
          if(writer != null) {
            writer.close();
          }
          current = clusterId;
          writer = new BufferedWriter(new FileWriter(
              new File(resultsPath + "/cluster-" + current + ".tsv")));
        }
        writer.write(stage.getAppId() + "\t" + stage.getStageId() + "\n");
      }
      if(writer != null) {
        writer.close();
      }
    } catch(Exception e) {
      e.printStackTrace();
    }

    // write centroid
    try {
      writer = new BufferedWriter(new FileWriter(
          new File(resultsPath + "/centroids.tsv")));
      cnt = 0;
      for(Stage stage: stageCentroids) {
        writer.write(cnt + "\t" + stage + "\n");
        cnt++;
      }
      writer.close();
    } catch(Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    int n = 30;
    if(args.length > 1) {
      n = Integer.parseInt(args[1]);
    }

    String resultPath = "cluster-results";
    if(args.length > 2) {
      resultPath = args[2];
    }

    StageClusterer obj = new StageClusterer(n, resultPath);
    obj.populateIgnoreApps();
    obj.populateIgnoreStages();

    obj.populateAllStages();
    obj.clusterStages();
    obj.cleanup();
  }

}
