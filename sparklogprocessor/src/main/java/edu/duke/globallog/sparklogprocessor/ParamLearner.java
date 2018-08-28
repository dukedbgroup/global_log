package edu.duke.globallog.sparklogprocessor;

import java.nio.file.*;
import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.stream.*;

import flanagan.analysis.Regression;

class AppStageExec {
    String appId;
    Long stageId;
    String execId;

    AppStageExec(String a, Long b, String c) {
      appId = a;
      stageId = b;
      execId = c;
    }

    String getAppId() { return appId; }
    Long getStageId() { return stageId; }
    String getExecId() { return execId; }
}

public class ParamLearner extends TrainingBase {

  int clusterId = 0;
  String resultsPath = "cluster-results"; // default
  List<AppStageExec> inputs = new ArrayList<AppStageExec>();
  RegressionResults results = new RegressionResults();

  ParamLearner(int n, String path) {
    clusterId = n;
    resultsPath = path;
    startConnection();
  }

  void cleanup() {
    commit();
    stopConnection();
  }

  void populateInputs() {
    String filePath = resultsPath + "/cluster-" + clusterId + ".tsv";
    String qsql = "SELECT execId from " + EXEC_TABLE + " where " +
         "appId=? and stageId=? GROUP BY execId";
    String appId = "";
    Long stageId = 0L;
    String execId = "";
    BufferedReader reader = null;
    try {
      PreparedStatement qstmt = newPreparedStatement(qsql);
      reader = new BufferedReader(new FileReader(filePath));
      String line = "";
      while((line = reader.readLine()) != null) {
        String[] arr = line.split("\t");
        appId = arr[0];
        stageId = Long.parseLong(arr[1]);
        qstmt.clearParameters();
        qstmt.setObject(1, appId);
        qstmt.setObject(2, stageId);
        ResultSet rs = qstmt.executeQuery();
        while(rs.next()) {
          execId = rs.getString("execId");
          inputs.add(new AppStageExec(appId, stageId, execId));
System.out.println("Adding: " + appId + ", " + stageId + ", " + execId);
        }
      }
      if(reader != null) {
        reader.close();
      }
      closeStatement(qstmt);
    } catch(Exception e) {
      e.printStackTrace();
    }    
  }

  // assumes statement with parameters (app, stage, exec) in that order
  ResultSet runPreparedStatement(PreparedStatement stmt, AppStageExec input) {
    try {
      stmt.clearParameters();
      stmt.setObject(1, input.getAppId());
      stmt.setObject(2, input.getStageId());
      stmt.setObject(3, input.getExecId());
      return stmt.executeQuery();
    } catch(Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  void runRegression() {
    // exec dataflow
    String qsql1 = "SELECT ipBytes as one, cachedBytes as two, "
      + "shuffleBytesWritten as three, shLocalBytesRead as four, "
      + "shRemoteBytesRead as five, opBytes as six, cacheBytesRead as seven"
      + " FROM " + EXEC_TABLE + " WHERE appId=? and stageId=? and execId=?";
    PreparedStatement qstmt1 = newPreparedStatement(qsql1);

    // task times
    String qsql2 = "SELECT min(launchTime) as one, max(finishTime) as two, "
      + "sum(memorySpilled) as three, sum(diskSpilled) as four"
      + " FROM " + TASK_METRICS_TABLE + " WHERE appId=? and stageId=? and executorId=?";
    PreparedStatement qstmt2 = newPreparedStatement(qsql2);
    String qsql3 = "SELECT min(launchTime) as one, max(finishTime) as two"
      + " FROM " + TASK_METRICS_TABLE + " WHERE appId=? and executorId=?";
    PreparedStatement qstmt3 = newPreparedStatement(qsql3);

    // perf stats
    String qsql4 = "SELECT maxStorageUsed as one, maxExecutionUsed as two, "
      + "(totalYoungCollection + totalOldCollection) as three"
      + " FROM " + PERF_MONITORS_TABLE + " WHERE appId=? and stageId=? and executorId=?";
    PreparedStatement qstmt4 = newPreparedStatement(qsql4);

    // regression parameters
    int numExamples = inputs.size();
    if(numExamples < 10) {
      System.out.println("Less than 10 examples provided!");
      return;
    }
    double[][] xArr1 = new double[7][numExamples];
    double[][] xArr2 = new double[2][numExamples];
    double[] yArr1 = new double[numExamples];
    double[] yArr2 = new double[numExamples];
    try {
      int cnt = 0;
      for(AppStageExec ip: inputs) {
        String appId = ip.getAppId();
        Long stageId = ip.getStageId();
        String execId = ip.getExecId();
        ResultSet rs1 = runPreparedStatement(qstmt1, ip);
        ResultSet rs2 = runPreparedStatement(qstmt2, ip);
        qstmt3.clearParameters();
        qstmt3.setObject(1, appId);
        qstmt3.setObject(2, execId);
        ResultSet rs3 = qstmt3.executeQuery();
        ResultSet rs4 = runPreparedStatement(qstmt4, ip);

        double ipBytes = 0d, cachedBytes = 0d, cacheBytesRead = 0d, shBytesWritten = 0d;
        double shLocalBytesRead = 0d, shRemoteBytesRead = 0d, opBytes = 0d;
        double memSpilled = 0d, diskSpilled = 0d;
        double globalStartTime = 0d, globalFinishTime = 0d;
        double stageStartTime = 0d, stageFinishTime = 0d;
        double maxStorage = 0d, maxExecution = 0d, totalCollection = 0d;
        double deltaStorage = 0d, deltaExecution = 0d, deltaHeap = 0d;

        while(rs1.next()) {
          ipBytes = rs1.getDouble("one");
          cachedBytes = rs1.getDouble("two");
          cacheBytesRead = rs1.getDouble("seven");
          shBytesWritten = rs1.getDouble("three");
          shLocalBytesRead = rs1.getDouble("four");
          shRemoteBytesRead = rs1.getDouble("five");
          opBytes = rs1.getDouble("six");
        }
        while(rs2.next()) {
          stageStartTime = rs2.getDouble("one");
          stageFinishTime = rs2.getDouble("two");
          memSpilled = rs2.getDouble("three");
          diskSpilled = rs2.getDouble("four");
        }
        while(rs3.next()) {
          globalStartTime = rs3.getDouble("one");
          globalFinishTime = rs3.getDouble("two");
        }
        while(rs4.next()) {
          maxStorage = rs4.getDouble("one");
          maxExecution = rs4.getDouble("two");
          totalCollection = rs4.getDouble("three");
        }

        // hard-coding file path pattern
        String fileToRead = PERF_MONITOR_HOME + appId + "/" + execId + "/" + PERF_FILE_PREFIX + appId + "_" + execId + ".txt";
        System.out.println("Reading file: " + fileToRead);
        Path path = Paths.get(fileToRead);
        Stream<String> lines = Files.lines(path);
        Long totalLines = lines.count() - 2; //first two header lines
        Double totalTime = globalFinishTime - globalStartTime;
        long startLine = Math.round((stageStartTime - globalStartTime) 
          * totalLines / totalTime);
        long lastLine = Math.min( Math.round((stageFinishTime - globalStartTime)
          * totalLines / totalTime), totalLines - 1);
        System.out.println("Reading lines: " + startLine + " to " + lastLine);
        try {
          lines = Files.lines(path);
          lines = lines.skip(2); // header lines
          if(startLine > 0L) {
            lines = lines.skip(startLine);
          }
          long currentLine = startLine;
          deltaStorage = deltaExecution = deltaHeap = 0d;
          double prevStorage = 0d;
          double prevExecution = 0d;
          Iterator<String> iterator = lines.iterator();
          while(iterator.hasNext()) {
            String line = iterator.next();
            String[] tokens = line.split("\t");
            double h = Double.parseDouble(tokens[6]);
            double s = Double.parseDouble(tokens[20]);
            double e = Double.parseDouble(tokens[21]);
            deltaStorage += Math.min(s-prevStorage, 0d);
            deltaExecution += Math.min(s-prevExecution, 0d);
            if(currentLine == startLine) {
              deltaHeap = h;
            } else if(currentLine == lastLine) {
              deltaHeap -= h;
              break;
            }
            prevStorage = s;
            prevExecution = e;
            currentLine++;
          }
        } catch(Exception e) {
          e.printStackTrace();
        }

        // input decompresssion
        xArr1[0][cnt] = ipBytes;
        // next two for serialization buffers 
        xArr1[1][cnt] = shBytesWritten + opBytes;
        // bytes cached to disk, only for mem and disk model
        // 0 for mem only model
        xArr1[2][cnt] = cachedBytes - deltaStorage;
        // next two for deserialization array
        xArr1[3][cnt] = cacheBytesRead;
        xArr1[4][cnt] = shLocalBytesRead + shRemoteBytesRead;
        // next two for network fetches
        xArr1[5][cnt] = shRemoteBytesRead;
        xArr1[6][cnt] = 0; // should be remote cache bytes read, but not available
        // next two are for execution memory model
        xArr2[0][cnt] = shBytesWritten;
        xArr2[1][cnt] = shLocalBytesRead + shRemoteBytesRead;
        // y value giving total bytes seen by JVM heap
        // subtracting storage and execution which are on accounted for on LHS
        yArr1[cnt] = deltaHeap + totalCollection - deltaStorage - deltaExecution;
        // execution memory needed
        yArr2[cnt] = deltaExecution + memSpilled;

        cnt++;
      } // loop over

        // check if any row is all zeroes
        int[] goodRows1 = findNonEmptyRows(xArr1);
        int[] goodRows2 = findNonEmptyRows(xArr2);
        double[][] xArr1new = new double[goodRows1.length][numExamples];
        double[][] xArr2new = new double[goodRows2.length][numExamples];
        int i = 0;
        for(int row: goodRows1) {
          System.arraycopy(xArr1[row], 0, xArr1new[i], 0, numExamples);
          i++;
        }
System.out.println("New Array 1: " + Arrays.deepToString(xArr1new));
        i = 0;
        for(int row: goodRows2) {
          System.arraycopy(xArr2[row], 0, xArr2new[i], 0, numExamples);
          i++;
        }
System.out.println("New Array 2: " + Arrays.deepToString(xArr2new));
        // run two regression models, handle exceptions 
        Regression reg1 = new Regression(xArr1new, yArr1);
        RegressionResults results1 = new RegressionResults();
        try {
          reg1.linear(0); // with 0 intercept
          double[] betaBars = reg1.getBestEstimates();
          double[] betaErrors = reg1.getBestEstimatesErrors();
          double df = reg1.getDegFree();
          double[][] cov = reg1.getCovMatrix();
          results1.setDegFree((int) df);
          results1.setNumVariables(goodRows1.length);
          results1.setVarNumbers(goodRows1);
          results1.setMeans(betaBars);
          results1.setStdDeviations(betaErrors);
          results1.setCoVar(cov);
    System.out.println("Beta bars: " + Arrays.toString(betaBars));
    System.out.println("Beta std dev: " + Arrays.toString(betaErrors));
    System.out.println("Degree freedom: " + df);
    System.out.println("Beta covariance: " + Arrays.deepToString(cov));
    System.out.println("Variables used: " + Arrays.toString(goodRows1));
        } catch(Exception e) {
          e.printStackTrace();
        }

        Regression reg2 = new Regression(xArr2new, yArr2);
        RegressionResults results2 = new RegressionResults();
        try {
          reg2.linear(0); // with 0 intercept
          double[] betaBars = reg2.getBestEstimates();
          double[] betaErrors = reg2.getBestEstimatesErrors();
          double df = reg2.getDegFree();
          double[][] cov = reg2.getCovMatrix();
          for(i=0; i<goodRows2.length; i++) {
            goodRows2[i] += 7; // these variables follow the variables in model 1
          }
          results1.setDegFree((int) df);
          results1.setNumVariables(goodRows2.length);
          results1.setVarNumbers(goodRows2);
          results1.setMeans(betaBars);
          results1.setStdDeviations(betaErrors);
          results1.setCoVar(cov);
    System.out.println("Beta bars: " + Arrays.toString(betaBars));
    System.out.println("Beta std dev: " + Arrays.toString(betaErrors));
    System.out.println("Degree freedom: " + df);
    System.out.println("Beta covariance: " + Arrays.deepToString(cov));
    System.out.println("Variables used: " + Arrays.toString(goodRows2));
        } catch(Exception e) {
          e.printStackTrace();
        }

      // merge two results 
      results.mergeResults(results1, results2);
      
      closeStatement(qstmt1);
      closeStatement(qstmt2);
      closeStatement(qstmt3);
      closeStatement(qstmt4);
    } catch(Exception e) {
      e.printStackTrace();
    }
  }

  int[] findNonEmptyRows(double[][] matrix) {
    Set<Integer> nonEmpty = new LinkedHashSet<Integer>();
    for(int i=0; i<matrix.length; i++) {
      boolean zero = true;
      for(int j=0; j<matrix[i].length; j++) {
        if(matrix[i][j] != 0d) { zero = false; }
      }
      if(!zero) { nonEmpty.add(i); }
    }
    return nonEmpty.stream().mapToInt(i->i).toArray();
  }

  void storeResults() {
     try(
        FileOutputStream fout = new FileOutputStream(resultsPath +
          "/params-" + clusterId + ".ser", true);
        ObjectOutputStream oos = new ObjectOutputStream(fout);
      ){
        oos.writeObject(results);
      } catch (Exception ex) {
         ex.printStackTrace();
      }
  }

  void sampleRegression() {
    double[][] xArray = { {1d, 2d, 1d, 1d, 2d, 2d},
                          {1d, 1d, 2d, 2d, 1d, 2d},
                          {0d, 0d, 0d, 0d, 0d, 0d},
                        };
    double[] yArray = {3d, 4d, 4d, 1d, 2d, 5d};
    Regression reg = new Regression(xArray, yArray);
    //reg.linear(0);
    reg.linearPlot(0);
    double[] betaBars = reg.getBestEstimates();
    double[] betaErrors = reg.getBestEstimatesErrors();
    double[] betaVariances = reg.getCoeffVar();
    double[] tValues = reg.getTvalues();
    double[] pValues = reg.getPvalues();
    double df = reg.getDegFree();
    double[][] cov = reg.getCovMatrix();
    double[] residuals = reg.getResiduals();
    double ss = reg.getSumOfSquares();
    System.out.println("Beta bars: " + Arrays.toString(betaBars));
    System.out.println("Beta std dev: " + Arrays.toString(betaErrors));
    System.out.println("Beta variation %: " + Arrays.toString(betaVariances));
    System.out.println("Beta t values: " + Arrays.toString(tValues));
    System.out.println("Beta p values: " + Arrays.toString(pValues));
    System.out.println("Degree freedom: " + df);
    System.out.println("Beta covariance: " + Arrays.deepToString(cov));
    System.out.println("Beta residuals: " + Arrays.toString(residuals));
    System.out.println("sum of squares: " + ss);
  }

  public static void main(String[] args) {
    int clusterId = 0;
    if(args.length >= 1) {
      clusterId = Integer.parseInt(args[0]);
    }
    String resultPath = "cluster-results";
    if(args.length >= 2) {
      resultPath = args[2];
    }

System.out.println("Running for cluster: " + clusterId);
    ParamLearner obj = new ParamLearner(clusterId, resultPath);
//    obj.sampleRegression();
    obj.populateInputs();
    obj.runRegression();
    obj.storeResults();
    obj.cleanup();
  }

}
