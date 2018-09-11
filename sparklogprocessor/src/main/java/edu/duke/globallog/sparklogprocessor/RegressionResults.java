package edu.duke.globallog.sparklogprocessor;

import java.io.*;

import cern.colt.matrix.tdouble.*;
import cern.colt.matrix.tdouble.*;
import cern.colt.matrix.tint.*;

public class RegressionResults implements Serializable {

  private static final long serialVersionUID = 2087368867376448459L;

  int DegFree;
  int numVariables;
  IntMatrix1D varNumbers;
  DoubleMatrix1D means;
  DoubleMatrix1D stdDeviations;
  DoubleMatrix2D coVar;
  
  public void setVarNumbers(int[] arr) {
    varNumbers = IntFactory1D.dense.make(arr);
  }

  public void setMeans(double[] arr) {
    means = DoubleFactory1D.dense.make(arr);
  }

  public void setStdDeviations(double[] arr) {
    stdDeviations = DoubleFactory1D.dense.make(arr);
  }

  public void setCoVar(double[][] arr) {
    coVar = DoubleFactory2D.dense.make(arr);
  }

  public void mergeResults(RegressionResults a, RegressionResults b) 
    throws IllegalArgumentException {
    if(a.getNumVariables() == 0 && b.getNumVariables() == 0) {
      return; // nothing to do
    }
    if(a.getNumVariables() == 0) {
      DegFree = b.getDegFree();
      numVariables = b.getNumVariables();
      varNumbers = b.getVarNumbers();
      means = b.getMeans();
      stdDeviations = b.getStdDeviations();
      coVar = b.getCoVar();
      return;
    }
    if(b.getNumVariables() == 0) {
      DegFree = a.getDegFree();
      numVariables = a.getNumVariables();
      varNumbers = a.getVarNumbers();
      means = a.getMeans();
      stdDeviations = a.getStdDeviations();
      coVar = a.getCoVar();
      return;
    }
    //if(a.getDegFree() != b.getDegFree()) {
    //  throw new IllegalArgumentException("Degrees of Freedom do not match");
    //}
    DegFree = Math.min(a.getDegFree(), b.getDegFree());
    numVariables = a.getNumVariables() + b.getNumVariables();
    varNumbers = IntFactory1D.dense.append(a.getVarNumbers(), b.getVarNumbers());
    means = DoubleFactory1D.dense.append(a.getMeans(), b.getMeans());
    stdDeviations = DoubleFactory1D.dense.append(a.getStdDeviations(), b.getStdDeviations());
    coVar = DoubleFactory2D.dense.composeDiagonal(a.getCoVar(), b.getCoVar());
  }

  /**
   * Get DegFree.
   *
   * @return DegFree as int.
   */
  public int getDegFree()
  {
      return DegFree;
  }
  
  /**
   * Set DegFree.
   *
   * @param DegFree the value to set.
   */
  public void setDegFree(int DegFree)
  {
      this.DegFree = DegFree;
  }
  
  /**
   * Get numVariables.
   *
   * @return numVariables as int.
   */
  public int getNumVariables()
  {
      return numVariables;
  }
  
  /**
   * Set numVariables.
   *
   * @param numVariables the value to set.
   */
  public void setNumVariables(int numVariables)
  {
      this.numVariables = numVariables;
  }
  
  /**
   * Get means.
   *
   * @return means as DoubleMatrix1D.
   */
  public DoubleMatrix1D getMeans()
  {
      return means;
  }
  
  /**
   * Set means.
   *
   * @param means the value to set.
   */
  public void setMeans(DoubleMatrix1D means)
  {
      this.means = means;
  }
  
  /**
   * Get stdDeviations.
   *
   * @return stdDeviations as DoubleMatrix1D.
   */
  public DoubleMatrix1D getStdDeviations()
  {
      return stdDeviations;
  }
  
  /**
   * Set stdDeviations.
   *
   * @param stdDeviations the value to set.
   */
  public void setStdDeviations(DoubleMatrix1D stdDeviations)
  {
      this.stdDeviations = stdDeviations;
  }
  
  /**
   * Get coVar.
   *
   * @return coVar as DoubleMatrix2D.
   */
  public DoubleMatrix2D getCoVar()
  {
      return coVar;
  }
  
  /**
   * Set coVar.
   *
   * @param coVar the value to set.
   */
  public void setCoVar(DoubleMatrix2D coVar)
  {
      this.coVar = coVar;
  }
  
  /**
   * Get varNumbers.
   *
   * @return varNumbers as IntMatrix1D.
   */
  public IntMatrix1D getVarNumbers()
  {
      return varNumbers;
  }
  
  /**
   * Set varNumbers.
   *
   * @param varNumbers the value to set.
   */
  public void setVarNumbers(IntMatrix1D varNumbers)
  {
      this.varNumbers = varNumbers;
  }
}
