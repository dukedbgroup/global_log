package edu.duke.globallog.sparklogprocessor;

import java.io.*;

import cern.colt.matrix.tdouble.DoubleFactory1D;
import cern.colt.matrix.tdouble.DoubleFactory2D;
import mikera.vectorz.Vector;

public class RegressionResults implements Serializable {

  int DegFree;
  int numVariables;
  DoubleFactory1D means;
  DoubleFactory1D stdDeviations;
  DoubleFactory2D coVar;
  
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
   * @return means as DoubleFactory1D.
   */
  public DoubleFactory1D getMeans()
  {
      return means;
  }
  
  /**
   * Set means.
   *
   * @param means the value to set.
   */
  public void setMeans(DoubleFactory1D means)
  {
      this.means = means;
  }
  
  /**
   * Get stdDeviations.
   *
   * @return stdDeviations as DoubleFactory1D.
   */
  public DoubleFactory1D getStdDeviations()
  {
      return stdDeviations;
  }
  
  /**
   * Set stdDeviations.
   *
   * @param stdDeviations the value to set.
   */
  public void setStdDeviations(DoubleFactory1D stdDeviations)
  {
      this.stdDeviations = stdDeviations;
  }
  
  /**
   * Get coVar.
   *
   * @return coVar as DoubleFactory2D.
   */
  public DoubleFactory2D getCoVar()
  {
      return coVar;
  }
  
  /**
   * Set coVar.
   *
   * @param coVar the value to set.
   */
  public void setCoVar(DoubleFactory2D coVar)
  {
      this.coVar = coVar;
  }
}
