package edu.duke.globallog.sparklogprocessor;

import java.io.*;
import java.sql.*;
import java.util.*;

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

  void sampleRegression() {
    double[][] xArray = { {1d, 2d, 1d, 1d, 2d, 2d},
                          {1d, 1d, 2d, 2d, 1d, 2d},
                          {1d, 1d, 1d, 2d, 2d, 1d},
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
    if(args.length > 1) {
      clusterId = Integer.parseInt(args[0]);
    }
    String resultPath = "cluster-results";
    if(args.length > 2) {
      resultPath = args[2];
    }

    ParamLearner obj = new ParamLearner(clusterId, resultPath);
    obj.populateInputs();
    obj.cleanup();
  }

}
