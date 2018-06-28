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
  Double[][] arrays;
  Integer[][] indices;
  Integer[] skyline;

  static int NUM_METRICS = 11;
  static boolean[] SORT_ORDER =
    {true, true, false, false, true, false, true, false, true, true, true};
  static int MAX_POST_MINIMAL = 10; // #entrries allowed beyong minimal in any dimension

  public SortableMetrics(List<Metrics> metrics) {
    this.metrics = metrics;
    buildIndex();
  }

  void buildIndex() {
    int count = metrics.size();
    arrays = new Double[NUM_METRICS][count];

    int i = 0;
    for(Metrics met: metrics) {
      arrays[0][i] = met.failedExecs;
      arrays[1][i] = met.failedTasks;
      arrays[2][i] = met.maxStorage;
      arrays[3][i] = met.maxExecution;
      arrays[4][i] = met.totalTime;
      arrays[5][i] = met.maxUsedHeap;
      arrays[6][i] = met.minUsageGap;
      arrays[7][i] = met.maxOldGenUsed;
      arrays[8][i] = met.totalGCTime;
      arrays[9][i] = met.totalNumYoungGC;
      arrays[10][i] = met.totalNumOldGC;
      i++;
    }

    indices = new Integer[NUM_METRICS][count];
    ArrayIndexComparator[] comparators = new ArrayIndexComparator[NUM_METRICS];
    for(i=0; i<NUM_METRICS; i++) {
      comparators[i] = new ArrayIndexComparator(arrays[i], SORT_ORDER[i]);
      indices[i] = comparators[i].createIndexArray();
      Arrays.sort(indices[i], comparators[i]);
    }
  }

  public Integer[] skyline(double epsilon) {
    int[] pointers = new int[NUM_METRICS];
    boolean[] done = new boolean[NUM_METRICS];
    double[] minimal = new double[NUM_METRICS];
    int[] post_minimal = new int[NUM_METRICS];
    for(int i=0; i<NUM_METRICS; i++) {
      pointers[i] = 0;
      done[i] = false;
      minimal[i] = -1.0;
      post_minimal[i] = -1;
    }

    // round robin search
    Set<Integer> skyList = new HashSet<Integer>();
    int doneCount = 0;
    int i=0;
    for( ; ; i++) {
      int nextList = i % NUM_METRICS;
      if(done[nextList]) {
        if(doneCount >= NUM_METRICS) { 
          break; 
        } else {
          continue;
        }
      }
      int nextCand = pointers[nextList]++;
      if(nextCand >= metrics.size()) {
System.out.println("-Done exhausted list: " + nextList + " at i=" + i);
        done[nextList] = true;
        doneCount++;
        continue;
      }
      Integer entry = indices[nextList][nextCand];
//System.out.println("-Looking candidate " + nextCand + " from list: " + nextList + " , entry: " + entry);
      if(skyList.contains(entry) && minimal[nextList] < 0d) {
System.out.println("-Minimal for #" + nextList + " is: " + arrays[nextList][entry] + " at i=" + i); 
        minimal[nextList] = arrays[nextList][entry];
        post_minimal[nextList] = 0;
      } else if(minimal[nextList] >= 0d &&
          Math.abs(1.0-(arrays[nextList][entry]/minimal[nextList])) > epsilon) {
System.out.println("-Done list: " + nextList + " at i=" + i);
        done[nextList] = true;
        doneCount++;
      } else if(!skyList.contains(entry)) {
        if(post_minimal[nextList] >= 0) {
          post_minimal[nextList]++;
        }
        if(post_minimal[nextList] >= MAX_POST_MINIMAL) {
System.out.println("-Done post minimal list: " + nextList + " at i=" + i); 
          done[nextList] = true;
          doneCount++;
        }
        skyList.add(entry);
      }
    }
System.out.println("--#indices parsed: " + i + " , #included in skylist:" + skyList.size());

    return skyList.toArray(new Integer[skyList.size()]);
  }


}
