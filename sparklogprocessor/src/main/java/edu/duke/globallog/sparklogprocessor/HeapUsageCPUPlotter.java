// Decompiled by Jad v1.5.8e2. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://kpdus.tripod.com/jad.html
// Decompiler options: packimports(3) fieldsfirst ansi space 
package edu.duke.globallog.sparklogprocessor;

import com.itextpdf.text.*;
import com.itextpdf.text.pdf.*;
import com.itextpdf.awt.DefaultFontMapper;
import java.awt.geom.Rectangle2D;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics2D;
import java.awt.GridLayout;
import java.awt.Rectangle;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;

import javax.swing.JPanel;

import org.jfree.chart.*;
import org.jfree.chart.axis.*;
import org.jfree.chart.plot.*;
import org.jfree.chart.renderer.xy.*;
import org.jfree.chart.title.TextTitle;
import org.jfree.data.time.*;
import org.jfree.data.xy.*;
import org.jfree.graphics2d.svg.SVGGraphics2D;
import org.jfree.graphics2d.svg.SVGUtils;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

public class HeapUsageCPUPlotter extends ApplicationFrame {

	// Hack: hard coding to 5GB
	private static Long MAX_PHYSICAL = 5*1024*1024*1024L;

	private static Boolean SPARK_POOLS = false;
	
	public static void main(String args[]) {
		
		
		String applicationID = args[0]; //"application_1446265188032_0144";
		String testName = "test63";
//		String gcAlgorithm = "G1";
		String gcAlgorithm = "Parallel GC";
		boolean showUsedCPU = true;

		if(args.length > 1 && "true".equals(args[1])) {
			SPARK_POOLS = true;
		}
		
		HeapUsageCPUPlotter multipleaxisdemo1 = new HeapUsageCPUPlotter(
				testName, applicationID,
				applicationID, gcAlgorithm, showUsedCPU);
		multipleaxisdemo1.pack();
		RefineryUtilities.centerFrameOnScreen(multipleaxisdemo1);
		multipleaxisdemo1.setVisible(true);
	}

	public HeapUsageCPUPlotter(String testName, String applicationID,
			String s, String gcAlgorithm, boolean showUsedCPU) {
		super(s);
		this.setLayout(new GridLayout(2, 5));

		File basedir = new File("/home/mayuresh/heap-logs/" + applicationID);
		File[] children = basedir.listFiles();
System.out.println("Base dir: " + basedir);
		Arrays.sort(children);
		for (int index = 0; index <= children.length - 1; index++) {
			if (children[index].isDirectory()) {

				FilenameFilter textFilter = new FilenameFilter() {
					public boolean accept(File dir, String name) {
						if (name.startsWith("sparkOutput_worker_application_")) {
							return true;
						} else {
							return false;
						}
					}
				};
				ChartPanel chartpanel = (ChartPanel) createDemoPanel(
						children[index].getName(),
						children[index].listFiles(textFilter)[0]
								.getAbsolutePath(), gcAlgorithm, showUsedCPU);
				chartpanel.setPreferredSize(new Dimension(600, 270));
				chartpanel.setDomainZoomable(true);
				chartpanel.setRangeZoomable(true);
				// setContentPane(chartpanel);
				add(chartpanel);
			}
		}
	}

	private static JFreeChart createChart(String executorID, String filename, String gcAlgorithm, boolean showUsedCPU) {
		ArrayList<XYDataset> xydatasets = createDataset(filename, gcAlgorithm);
		// create stacked area plot
		XYPlot xyplot = new XYPlot();
		xyplot.setDataset(0, xydatasets.get(0));
		xyplot.setDomainAxis(new NumberAxis("Sample index"));
		xyplot.setRangeAxis(0, new NumberAxis("Memory (Bytes)"));
		xyplot.setRenderer(0, new StackedXYAreaRenderer(XYAreaRenderer.AREA));
		//JFreeChart jfreechart = ChartFactory.createStackedXYAreaChart("Resource Usage",
		//		"Sample index", "Memory (Bytes)", xydatasets.get(0),
		//		PlotOrientation.VERTICAL, true, true, false);
		//jfreechart.addSubtitle(new TextTitle("Executor " + executorID));
		//XYPlot xyplot = (XYPlot) jfreechart.getPlot();
		xyplot.setOrientation(PlotOrientation.VERTICAL);
		xyplot.setDomainPannable(true);
		xyplot.setRangePannable(true);
		xyplot.setSeriesRenderingOrder(SeriesRenderingOrder.FORWARD);
		xyplot.setDatasetRenderingOrder(DatasetRenderingOrder.FORWARD);

		XYDataset xydataset;
		int index;
		for (index = 1; index < xydatasets.size(); index++) {

			xydataset = xydatasets.get(index);
			xyplot.setDataset(index, xydataset);
			// xyplot.mapDatasetToRangeAxis(1, 1);
                  if(!"Used CPU".equals(xydataset.getSeriesKey(0))) {
			XYItemRenderer xyitemrenderer; 
			xyitemrenderer = new StandardXYItemRenderer();
                	xyitemrenderer.setBaseStroke(new java.awt.BasicStroke(3));
                        xyitemrenderer.setSeriesStroke(0, new java.awt.BasicStroke(1.0f, java.awt.BasicStroke.CAP_BUTT, java.awt.BasicStroke.JOIN_MITER, 10.0f, new float[] {10.0f}, 0.0f));
                        xyplot.setRenderer(index, xyitemrenderer);
		  } else {	
			NumberAxis axis2 = new NumberAxis("CPU (%)");
			// axis2.setFixedDimension(10.0);
			axis2.setAutoRangeIncludesZero(false);
			axis2.setUpperBound(100);
			// axis2.setLabelPaint(Color.red);
			// axis2.setTickLabelPaint(Color.red);
			xyplot.setRangeAxis(index, axis2);
			// xyplot.setRangeAxisLocation(1, AxisLocation.BOTTOM_OR_LEFT);

			//xyplot.setDataset(index, xydatasets.get(index));
			xyplot.mapDatasetToRangeAxis(index, index);
			XYItemRenderer renderer2 = new StandardXYItemRenderer();
			xyplot.setRenderer(index, renderer2);
		  }
		}
		String name = "Executor: " + filename.substring(filename.lastIndexOf("application")).substring(12);
		name = name.substring(0, name.length()-4);
		JFreeChart jfreechart = new JFreeChart(name, xyplot);
		// jfreechart.removeLegend();
//              jfreechart.addSubtitle(new TextTitle("Executor: " + filename.substring(filename.lastIndexOf("application")).substring(12)));
		ChartUtilities.applyCurrentTheme(jfreechart);
                // jfreechart.getLegend().setItemFont(new java.awt.Font("Arial", java.awt.Font.BOLD, 16));
		xyplot.getRenderer().setSeriesPaint(0, Color.black);
//                xyplot.getRenderer(1).setSeriesPaint(0, Color.magenta);
//                xyplot.getRenderer(2).setSeriesPaint(0, Color.cyan);
		if(SPARK_POOLS) {
	                xyplot.getRenderer().setSeriesPaint(0, Color.PINK);
	                xyplot.getRenderer().setSeriesPaint(1, Color.ORANGE);
	                xyplot.getRenderer(1).setSeriesPaint(0, Color.BLACK);
	                xyplot.getRenderer(2).setSeriesPaint(0, Color.BLUE);
                        xyplot.getRenderer(3).setSeriesPaint(0, Color.GREEN);
		}
		xyplot.getRenderer(1).setBaseStroke(new java.awt.BasicStroke(5.0f));
                xyplot.getRenderer(2).setBaseStroke(new java.awt.BasicStroke(5.0f));
		return jfreechart;
	}

private static void writeAsPDF( JFreeChart chart, FileOutputStream out, int width, int height ) 
{ 
try 
{ 
com.itextpdf.text.Rectangle pagesize = new com.itextpdf.text.Rectangle( width, height ); 
Document document = new Document( pagesize, 50, 50, 50, 50 ); 
PdfWriter writer = PdfWriter.getInstance( document, out ); 
document.open(); 
PdfContentByte cb = writer.getDirectContent(); 
PdfTemplate tp = cb.createTemplate( width, height ); 
Graphics2D g2 = tp.createGraphics( width, height, new DefaultFontMapper() ); 
Rectangle2D r2D = new Rectangle2D.Double(0, 0, width, height ); 
chart.draw(g2, r2D); 
g2.dispose(); 
cb.addTemplate(tp, 0, 0); 
document.close(); 
} 
catch (Exception e) 
{ 
 e.printStackTrace();
} 
}

	private static void writeToPNG(JFreeChart chart, FileOutputStream out, int width, int height) {
		try {
		  ChartUtilities.writeChartAsPNG(out, chart, width, height);
		}
                catch (Exception e)
                {
                         e.printStackTrace();
                }
	}

	private static void writeToSVG( JFreeChart chart, File file, int width, int height )
	{
		try {
	        SVGGraphics2D g2 = new SVGGraphics2D(width, height);
	        Rectangle r = new Rectangle(0, 0, width, height);
	        chart.draw(g2, r);
        	SVGUtils.writeToSVG(file, g2.getSVGElement());
		}
		catch (Exception e)
		{	
			 e.printStackTrace();
		}
	}


	private static ArrayList<XYDataset> createDataset(String filename,
			String gcAlgorithm) {

		boolean java8 = true;

		if (gcAlgorithm.equals("G1")) {
			XYSeries G1OldGen = new XYSeries("G1 Old Gen");
			XYSeries G1YoungGen = new XYSeries("G1 Young Gen");
			XYSeries UsedHeap = new XYSeries("Used Heap");
			XYSeries CommittedHeap = new XYSeries("Committed Heap");
			XYSeries UsedCPU = new XYSeries("Used CPU");

			String line;
			long index = 1;
			try {
				BufferedReader br = new BufferedReader(
						new InputStreamReader(new FileInputStream(filename),
								Charset.forName("UTF-8")));
				while ((line = br.readLine()) != null) {
					// Deal with the line
					if (!line.contains("application") && !line.contains("Code")) {
						String tokens[] = line.split("\t");
						G1OldGen.add(index, Long.valueOf(tokens[3]));
						G1YoungGen.add(
								index,
								Long.valueOf(tokens[1])
										+ Long.valueOf(tokens[2])
										+ Long.valueOf(tokens[3]));
						UsedHeap.add(index, Long.valueOf(tokens[5]));
						CommittedHeap.add(index, Long.valueOf(tokens[6]));
						if (tokens.length >= 9)
							UsedCPU.add(index,
									Double.valueOf(tokens[tokens.length - 2]));

						index++;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			ArrayList<XYDataset> xyDatasets = new ArrayList<XYDataset>();
			XYSeriesCollection sc;
			sc = new XYSeriesCollection();
			sc.addSeries(G1OldGen);
			xyDatasets.add(sc);
			sc = new XYSeriesCollection();
			sc.addSeries(G1YoungGen);
			xyDatasets.add(sc);
			sc = new XYSeriesCollection();
			sc.addSeries(UsedHeap);
			xyDatasets.add(sc);
			sc = new XYSeriesCollection();
			sc.addSeries(CommittedHeap);
			xyDatasets.add(sc);
			sc = new XYSeriesCollection();
			sc.addSeries(UsedCPU);
			xyDatasets.add(sc);
			return xyDatasets;
		} else if (gcAlgorithm.equals("Parallel GC")) {
			XYSeries PSOldGen = new XYSeries("Old Gen",false,false);
			XYSeries PSYoungGen = new XYSeries("Young Gen",false,false);
			XYSeries PermGen = new XYSeries("JVM Internal",false,false);
			XYSeries OffHeap = new XYSeries("Used Off-heap",false,false);
			XYSeries MaxHeap = new XYSeries("Max Heap",false,false);
			// XYSeries UsedOffHeap = new XYSeries("Used OffHeap");
			XYSeries MaxPhysical = new XYSeries("Max Physical",false,false);
			XYSeries UsedCPU = new XYSeries("Used CPU",false,false);
                        XYSeries TotalMem = new XYSeries("Resident Set Size",false,false);

			XYSeries Storage = new XYSeries("Storage", false, false);
                        XYSeries Execution = new XYSeries("Execution", false, false);
                        XYSeries UsedHeap = new XYSeries("Used Heap", false, false);
			String line;
			long index = 1;
			try {
				BufferedReader br = new BufferedReader(
						new InputStreamReader(new FileInputStream(filename),
								Charset.forName("UTF-8")));
				while ((line = br.readLine()) != null) {
					// Deal with the line
					if (!line.contains("application") && !line.contains("Code")) {
						String tokens[] = line.split("\t");
						if(java8) { PSOldGen.add(index, Long.valueOf(tokens[5])); }
						else { PSOldGen.add(index, Long.valueOf(tokens[3])); }
						if(java8) { PSYoungGen.add(index, Long.valueOf(tokens[3]) + Long.valueOf(tokens[4])); }
						else { PSYoungGen.add(
								index,
								Long.valueOf(tokens[1])
										+ Long.valueOf(tokens[2])); }
						if(java8) { PermGen.add(index, Long.valueOf(tokens[0]) + Long.valueOf(tokens[1]) + Long.valueOf(tokens[2])); }
						else { PermGen.add(index, Long.valueOf(tokens[4]) + Long.valueOf(tokens[0])); }
						Long maxHeap = 0L;
						if(java8) { MaxHeap.add(index, Long.valueOf(tokens[8])); maxHeap = Long.valueOf(tokens[8]); }
						else { MaxHeap.add(index, Long.valueOf(tokens[7])); maxHeap = Long.valueOf(tokens[7]); }
						if(java8) { OffHeap.add(index, Long.valueOf(tokens[12])); }
						else { OffHeap.add(index, Long.valueOf(tokens[11])); }
						if(java8) { UsedCPU.add(index, Math.max(0.0, Double.valueOf(tokens[15]))); }
						else { UsedCPU.add(index, Math.max(0.0, Double.valueOf(tokens[14]))); }
						if(java8) { TotalMem.add(index, Double.valueOf(tokens[14])); }
						else { TotalMem.add(index, Double.valueOf(tokens[13])); }
						if(java8) { 
                                                  UsedHeap.add(index, Long.valueOf(tokens[0]) + Long.valueOf(tokens[1]) + Long.valueOf(tokens[2]) + Long.valueOf(tokens[3]) + Long.valueOf(tokens[4]) + Long.valueOf(tokens[5])); }
						else {
						  UsedHeap.add(index, Long.valueOf(tokens[0]) + Long.valueOf(tokens[1]) + Long.valueOf(tokens[2]) + Long.valueOf(tokens[3]) + Long.valueOf(tokens[4])); }
						MaxPhysical.add(index, maxHeap + 1024*1024*1024); // MaxPhysical.add(index, MAX_PHYSICAL);
						if(java8) {
						  Storage.add(index, Long.valueOf(tokens[20]));
						  Execution.add(index, Long.valueOf(tokens[21]));
						} else {
                                                  Storage.add(index, Long.valueOf(tokens[19]));
                                                  Execution.add(index, Long.valueOf(tokens[20]));
						}
					
						index++;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			ArrayList<XYDataset> xyDatasets = new ArrayList<XYDataset>();
			XYSeriesCollection sc;
			//sc = new XYSeriesCollection();
			//sc.addSeries(PSOldGen);
			//xyDatasets.add(sc);
			//sc = new XYSeriesCollection();
			//sc.addSeries(PSYoungGen);
			//xyDatasets.add(sc);
			//sc = new XYSeriesCollection();
			//sc.addSeries(PermGen);
			//xyDatasets.add(sc);

		  if(!SPARK_POOLS) {
			DefaultTableXYDataset td = new DefaultTableXYDataset();
			td.addSeries(PSOldGen);
			td.addSeries(PSYoungGen);
			td.addSeries(PermGen);
			td.addSeries(OffHeap);
			xyDatasets.add(td);

			td = new DefaultTableXYDataset();
			td.addSeries(UsedCPU);
			xyDatasets.add(td);
                        // sc = new XYSeriesCollection();
                        // sc.addSeries(UsedOffHeap);
                        // xyDatasets.add(sc);
                        td = new DefaultTableXYDataset();
                        td.addSeries(TotalMem);
                        xyDatasets.add(td);
                        td = new DefaultTableXYDataset();
                        td.addSeries(MaxPhysical);
                        xyDatasets.add(td);
 			td = new DefaultTableXYDataset();
			td.addSeries(MaxHeap);
			xyDatasets.add(td);
		  } else {
                        DefaultTableXYDataset td = new DefaultTableXYDataset();
                        td.addSeries(Storage);
                        td.addSeries(Execution);
                        xyDatasets.add(td);
                        td = new DefaultTableXYDataset();
                        td.addSeries(PSOldGen);
                        xyDatasets.add(td);
                        td = new DefaultTableXYDataset();
                        td.addSeries(UsedHeap);
                        xyDatasets.add(td);
                        td = new DefaultTableXYDataset();
                        td.addSeries(MaxHeap);
                        xyDatasets.add(td);
 		  }
			return xyDatasets;
		}

		return null;
	}

	public static JPanel createDemoPanel(String executorID, String filename,
			String gcAlgorithm, boolean showUsedCPU) {
		JFreeChart jfreechart = createChart(executorID, filename, gcAlgorithm, showUsedCPU);
	try {
		String name = "/home/mayuresh/pics/" + filename.substring(filename.lastIndexOf("application"));
		name = name.substring(0, name.length()-4); // take away .txt extension
		if(SPARK_POOLS) {
			name += "-spark";
		}
//		writeToSVG(jfreechart, new java.io.File(name + ".svg"), 640, 480);
		writeAsPDF(jfreechart, new FileOutputStream(name + ".pdf"), 640, 480);
                writeToPNG(jfreechart, new FileOutputStream(name + ".png"), 640, 480);
	} catch(Exception e) {
		e.printStackTrace();
	}
		ChartPanel chartpanel = new ChartPanel(jfreechart);
		chartpanel.setMouseWheelEnabled(true);

		return chartpanel;
	}
}
