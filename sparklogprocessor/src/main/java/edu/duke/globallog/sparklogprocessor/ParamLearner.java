package edu.duke.globallog.sparklogprocessor;

import java.io.*;
import java.sql.*;
import java.util.*;

import flanagan.analysis.Regression;

public class ParamLearner extends TrainingBase {

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
    if(args.length > 2) {
      
    }
    ParamLearner obj = new ParamLearner();
    obj.sampleRegression();
  }

}
