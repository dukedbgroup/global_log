package edu.duke.globallog.sparklogprocessor;

import java.util.*;

class ArrayIndexComparator implements Comparator<Integer>
{
    private final Double[] array;
    boolean sort_asc = true;

    public ArrayIndexComparator(Double[] array, boolean sort_asc)
    {
        this.array = array;
        this.sort_asc = sort_asc;
    }

    public Integer[] createIndexArray()
    {
        Integer[] indexes = new Integer[array.length];
        for (int i = 0; i < array.length; i++)
        {
            indexes[i] = i; // Autoboxing
        }
        return indexes;
    }

    @Override
    public int compare(Integer index1, Integer index2)
    {
         // Autounbox from Integer to int to use as array indexes
        if(sort_asc) {
          return array[index1].compareTo(array[index2]);
        } else {
          return array[index2].compareTo(array[index1]);
        }
    }
}

public class SortableMetrics {

  List<Metrics> metrics;
  Integer[][] indices;
  Integer[] skyline;

  static int NUM_METRICS = 10;
  static boolean[] SORT_ORDER =
    {true, false, false, true, false, true, false, true, true, true};

  public SortableMetrics(List<Metrics> metrics) {
    this.metrics = metrics;
    buildIndex();
  }

  void buildIndex() {
    int count = metrics.size();
    Double[][] arrays = new Double[NUM_METRICS][count];

    int i = 0;
    for(Metrics met: metrics) {
      arrays[0][i] = met.failedExecs;
      arrays[1][i] = met.maxStorage;
      arrays[2][i] = met.maxExecution;
      arrays[3][i] = met.totalTime;
      arrays[4][i] = met.maxUsedHeap;
      arrays[5][i] = met.minUsageGap;
      arrays[6][i] = met.maxOldGenUsed;
      arrays[7][i] = met.totalGCTime;
      arrays[8][i] = met.totalNumYoungGC;
      arrays[9][i] = met.totalNumOldGC;
      i++;
    }

    indices = new Integer[NUM_METRICS][count];
    ArrayIndexComparator[] comparators = new ArrayIndexComparator[NUM_METRICS];
    for(i=0; i<10; i++) {
      comparators[i] = new ArrayIndexComparator(arrays[i], SORT_ORDER[i]);
      indices[i] = comparators[i].createIndexArray();
      Arrays.sort(indices[i], comparators[i]);
    }
  }

  public Integer[] skyline(double epsilon) {
    int[] pointers = new int[NUM_METRICS];
    boolean[] done = new boolean[NUM_METRICS];
    for(int i=0; i<NUM_METRICS; i++) {
      pointers[i] = 0;
      done[i] = false;
    }

    // round robin search
    List<Integer> skyList = new ArrayList<Integer>();
    int doneCount = 0;
    for(int i=0; ; i++) {
      int nextList = i % NUM_METRICS;
      if(done[nextList]) {
        if(doneCount >= NUM_METRICS) { 
          break; 
        } else {
          continue;
        }
      }
      int nextCand = pointers[nextList]++;
      Integer entry = indices[nextList][nextCand];
System.out.println("-Looking candidate " + nextCand + " from list: " + nextList + " , entry: " + entry);
      if(skyList.contains(entry)) {
System.out.println("-Done list: " + nextList);
        done[nextList] = true;
        doneCount++;
      } else {
        skyList.add(entry);
      }
    }

    return skyList.toArray(new Integer[skyList.size()]);
  }


}
