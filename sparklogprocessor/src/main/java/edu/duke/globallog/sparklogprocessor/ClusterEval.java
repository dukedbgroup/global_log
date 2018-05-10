package edu.duke.globallog.sparklogprocessor;

import java.util.*;

import org.apache.commons.math3.util.CombinatoricsUtils;

import weka.clusterers.*;
import weka.core.*;

public class ClusterEval {

  List<Integer> clusterClasses;
  List<Integer> clusterIds;

  long TP, TN, FP, FN;

  public void setClusterClasses(List<Integer> list) {
    clusterClasses = list;
  }

  public void setClusterIds(List<Integer> list) {
    clusterIds = list;
  }

  boolean isConsistent() {
    if(clusterClasses != null && clusterIds != null &&
       clusterClasses.size() == clusterIds.size() &&
       Collections.max(clusterClasses) == Collections.max(clusterIds)) {
         return true;
    }
try {
System.out.println("!!!!Inconsistent:\n Size of cluster classes: " + clusterClasses.size() + " max: " + Collections.max(clusterClasses) + "\n Size of cluster ids: " + clusterIds.size() + " max: " + Collections.max(clusterIds));
} catch(Exception e) {

}
    return false;
  }

  public double relativeCondEntropy() {
    return conditionalEntropy() / Math.log(clusterIds.size());
  }

  public double conditionalEntropy() {
    return jointEntropy() - clusterEntropy();
  }

  public double jointEntropy() {
    if(!isConsistent()) {
      return -1;
    }
    Map<Integer, Map<Integer, Integer>> clusterPartCounts = 
      new HashMap<Integer, Map<Integer,Integer>>();
    int cnt = 0;
    for(Integer id: clusterIds) {
      Integer part = clusterClasses.get(cnt++);
      if(clusterPartCounts.containsKey(id)) {
        Map<Integer, Integer> partCounts = clusterPartCounts.get(id);
        if(partCounts.containsKey(part)) {
          partCounts.put(part, 1 + partCounts.get(part));
        } else {
          partCounts.put(part, 1);
        }
      } else {
        Map<Integer, Integer> partCounts = new HashMap<Integer, Integer>();
        partCounts.put(part, 1);
        clusterPartCounts.put(id, partCounts);
      }
    }

    int total = clusterIds.size();
    double entropy = 0d;
    for(Integer id: clusterPartCounts.keySet()) {
      Map<Integer, Integer> partCounts = clusterPartCounts.get(id);
      for(Integer part: partCounts.keySet()) {
        entropy += entropyComponent(partCounts.get(part), total);
//System.out.println("Joint entropy: " + entropy);
      }
    }
    return entropy;
  }

  public double clusterEntropy() {
    if(!isConsistent()) {
      return -1;
    }
    Map<Integer, Integer> clusterCounts = new HashMap<Integer, Integer>();
    for(Integer id: clusterIds) {
      if(clusterCounts.containsKey(id)) {
        clusterCounts.put(id, 1 + clusterCounts.get(id));
      } else {
        clusterCounts.put(id, 1);
      }
    }

    int total = clusterIds.size();
    double entropy = 0d;
    for(Integer id: clusterCounts.keySet()) {
      entropy += entropyComponent(clusterCounts.get(id), total);
//System.out.println("Cluster entropy: " + entropy);
    }
    return entropy;
  }

  public double partitionEntropy() {
    if(!isConsistent()) {
      return -1;
    }
    Map<Integer, Integer> partCounts = new HashMap<Integer, Integer>();
    for(Integer id: clusterClasses) {
      if(partCounts.containsKey(id)) {
        partCounts.put(id, 1 + partCounts.get(id));
      } else {
        partCounts.put(id, 1);
      }
    }

    int total = clusterClasses.size();
    double entropy = 0d;
    for(Integer id: partCounts.keySet()) {
      entropy += entropyComponent(partCounts.get(id), total);
//System.out.println("Partition entropy: " + entropy);
    }
    return entropy;

  }

  public double mutualInformation() {
    return clusterEntropy() + partitionEntropy() - jointEntropy();
/*
    if(!isConsistent()) {
      return -1;
    }
    Map<Integer, Integer> clusterCounts = new HashMap<Integer, Integer>();
    Map<Integer, Integer> partCounts = new HashMap<Integer, Integer>();
    Map<Integer, Map<Integer, Integer>> clusterPartCounts =
      new HashMap<Integer, Map<Integer,Integer>>();

    int cnt = 0;
    for(Integer id: clusterIds) {

      if(clusterCounts.containsKey(id)) {
        clusterCounts.put(id, 1 + clusterCounts.get(id));
      } else {
        clusterCounts.put(id, 1);
      }

      Integer part = clusterClasses.get(cnt++);
      if(clusterPartCounts.containsKey(id)) {
        Map<Integer, Integer> pCounts = clusterPartCounts.get(id);
        if(pCounts.containsKey(part)) {
          pCounts.put(part, 1 + pCounts.get(part));
        } else {
          pCounts.put(part, 1);
        }
      } else {
        Map<Integer, Integer> pCounts = new HashMap<Integer, Integer>();
        pCounts.put(part, 1);
        clusterPartCounts.put(id, pCounts);
      }

    }

    for(Integer id: clusterClasses) {
      if(partCounts.containsKey(id)) {
        partCounts.put(id, 1 + partCounts.get(id));
      } else {
        partCounts.put(id, 1);
      }
    }

    int total = clusterIds.size();
    double info = 0d;
    for(Integer id: clusterPartCounts.keySet()) {
      Map<Integer, Integer> pCounts = clusterPartCounts.get(id);
      for(Integer part: pCounts.keySet()) {
        info += 1.0 * pCounts.get(part) / total * 
          Math.log((pCounts.get(part) * total) / (clusterCounts.get(id) * partCounts.get(part)));
System.out.println("Mutual entropy: " + info);
      }
    }
    return info;
*/
  }

  public double normMutualInfo() {
    double clustE = clusterEntropy();
    double partE = partitionEntropy();
    double mutualI = mutualInformation();
    return 2 * mutualI / (clustE + partE);
  }

  private double entropyComponent(int qualified, int total) {
    if(qualified <= total) {
      double ratio = (double) qualified / total;
      return -1.0 * ratio * Math.log(ratio);
    }
    return -1.0;
  }

  public void pairEval() {
    if(!isConsistent()) {
      return;
    }

    Map<Integer, Integer> classCounts = new HashMap<Integer, Integer>();
    Map<Integer, Map<Integer, Integer>> clusterCounts = 
      new HashMap<Integer, Map<Integer, Integer>>();
    int curIndex = 0;
    for(int i=0; i<clusterClasses.size(); i++) {
      int classId = clusterClasses.get(i);
      int clusterId = clusterIds.get(i);
      if(classCounts.containsKey(classId)) {
        classCounts.put(classId, 1 + classCounts.get(classId));
        Map<Integer, Integer> clusterMap = clusterCounts.get(classId);
        if(clusterMap.containsKey(clusterId)) {
          clusterMap.put(clusterId, 1 + clusterMap.get(clusterId));
        } else {
          clusterMap.put(clusterId, 1);
        }
      } else {
        classCounts.put(classId, 1);
        Map<Integer, Integer> clusterMap = new HashMap<Integer, Integer>();
        clusterMap.put(clusterId, 1);
        clusterCounts.put(classId, clusterMap);
      }
    }

System.out.println("Counts: " + classCounts + " \nMsps: " + clusterCounts);

    long tpPlusFp = 0L;
    for(Integer count: classCounts.values()) {
      if(count > 1) {
        tpPlusFp += CombinatoricsUtils.binomialCoefficient(count, 2);
      } else {
        //tpPlusFp ++;
      }
    }

    TP = 0L;
    for(Map<Integer, Integer> clusterMap: clusterCounts.values()) {
      if(clusterMap.size() > 1) {
        for(Integer count: clusterMap.values()) {
          if(count > 1) {
            TP += CombinatoricsUtils.binomialCoefficient(count, 2);
          }
        }
      } else {
        int count = clusterMap.values().iterator().next();
        if(count > 1) {
          TP += CombinatoricsUtils.binomialCoefficient(count, 2);
        } else {
          //TP ++;
        }
      }
    }

    FP = tpPlusFp - TP;

    long tnPlusFn = 0L;
    for(int i = 0; i < classCounts.size()-1; i++) {
      for(int j = i+1; j < classCounts.size(); j++) {
        tnPlusFn += Long.valueOf(classCounts.get(i)) * classCounts.get(j);
      } 
    }

    FN = 0L;
    for(int i = 0; i < classCounts.size()-1; i++) {
      for(int j = i+1; j < classCounts.size(); j++) {
        Map<Integer, Integer> clusterMapI = clusterCounts.get(i);
        Map<Integer, Integer> clusterMapJ = clusterCounts.get(j);
        for(Integer key: clusterMapI.keySet()) {
          if(clusterMapJ.containsKey(key)) { //match found
            FN += Long.valueOf(clusterMapI.get(key)) * clusterMapJ.get(key);
          }
        }
      }
    } 

    TN = tnPlusFn - FN;
  }

  public double randIndex() {
    // rand index
    return Double.valueOf(TP + TN) / Double.valueOf(TP + FP + TN + FN);
  }

  public double jaccard() {
    return Double.valueOf(TP) / Double.valueOf(TP + FN + FP);
  }

  public double fowlkesMallow() {
    return Double.valueOf(TP) / Math.sqrt(
      Double.valueOf(TP + FN) * Double.valueOf(TP + FP));
  }

  public long truePositives() {
    return TP;
  }

  public long trueNegatives() {
    return TN;
  }

  public long falsePositives() {
    return FP;
  }

  public long falseNegatives() {
    return FN;
  }

}
