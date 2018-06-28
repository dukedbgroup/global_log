package edu.duke.globallog.sparklogprocessor;

import java.util.ArrayList;
import java.util.List;

import weka.clusterers.*;
import weka.core.*;

public class DataClusterer {

  SimpleKMeans object;
  Instances data;
  int[] answers;
  Instances centroids;

  public DataClusterer() {
    object = new SimpleKMeans();
  }

  public void cluster(int n) {
    try {
      object.setPreserveInstancesOrder(true);
      object.setNumClusters(n);
//      object.setDistanceFunction(new ManhattanDistance(data));
      object.buildClusterer(data);
      answers = object.getAssignments();
//System.out.println("Clustering output: " + java.util.Arrays.toString(answers));
      centroids = object.getClusterCentroids();
    } catch(Exception e) {
      e.printStackTrace();
    }
  }

  public int[] getAnswers() {
    return answers;
  }

  public Stage[] getCentroids() {
    Stage[] output = new Stage[centroids.numInstances()];
    for(int i=0; i < centroids.numInstances(); i++) {
      Instance instance = centroids.instance(i);
      output[i] = new Stage((long)instance.value(0), (long)instance.value(1), 
        (long)instance.value(2), (long)instance.value(3),
        (long)instance.value(4), (long)instance.value(5));
    }
    return output;
  }

  void setMetrics(List<Metrics> metList) {
    ArrayList<Attribute> attList = new ArrayList<Attribute>();
    attList.add(new Attribute("failedExecs", 0));
    attList.add(new Attribute("failedTasks", 1)); 
    attList.add(new Attribute("maxStorage", 2));
    attList.add(new Attribute("maxExecution", 3));
    attList.add(new Attribute("totalTime", 4));
    attList.add(new Attribute("maxUsedHeap", 5));
    attList.add(new Attribute("minUsageGap", 6));
    attList.add(new Attribute("maxOldGenUsed", 7));
    attList.add(new Attribute("totalGCTime", 8));
    attList.add(new Attribute("totalNumYoungGC", 9));
    attList.add(new Attribute("totalNumOldGC", 10));
    data = new Instances("data", attList, metList.size());
    for(Metrics met: metList) {
      Instance inst = new DenseInstance(11); //HACK: hard-coding metrics size
//      inst.setValue(3, 0);
      inst.setValue(0, met.failedExecs);
      inst.setValue(1, met.failedTasks);
      inst.setValue(2, met.maxStorage);
      inst.setValue(3, met.maxExecution);
      inst.setValue(4, met.totalTime);
      inst.setValue(5, met.maxUsedHeap);
      inst.setValue(6, met.minUsageGap);
      inst.setValue(7, met.maxOldGenUsed);
      inst.setValue(8, met.totalGCTime);
      inst.setValue(9, met.totalNumYoungGC);
      inst.setValue(10, met.totalNumOldGC);
      // set the reference
      inst.setDataset(data);
      data.add(inst);
    }
  }

  void setStages(List<Stage> stageList) {
    ArrayList<Attribute> attList = new ArrayList<Attribute>();
    attList.add(new Attribute("ipBytes", 0));
    attList.add(new Attribute("cachedBytes", 1));
    attList.add(new Attribute("shBytesRead", 2));
    attList.add(new Attribute("shBytesWritten", 3));
    attList.add(new Attribute("opBytes", 4));
    attList.add(new Attribute("cacheStorage", 5));
    data = new Instances("data", attList, stageList.size());
    for(Stage met: stageList) {
      Instance inst = new DenseInstance(6); //HACK: hard-coding metrics size
      inst.setValue(0, met.ipBytes);
      inst.setValue(1, met.cachedBytes);
      inst.setValue(2, met.shuffleBytesRead);
      inst.setValue(3, met.shuffleBytesWritten);
      inst.setValue(4, met.opBytes);
      inst.setValue(5, met.cacheStorage);
      // set the reference
      inst.setDataset(data);
      data.add(inst);
    }    
  }

}
