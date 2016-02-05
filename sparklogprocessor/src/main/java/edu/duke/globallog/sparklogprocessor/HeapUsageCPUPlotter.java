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

		File basedir = new File(applicationID);
		File[] children = basedir.listFiles();
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
			// renderer2.setSeriesPaint(0, Color.red);
			 ;
			xyplot.setRenderer(index, renderer2);
		  }
		}
		JFreeChart jfreechart = new JFreeChart("Executor: " + filename.substring(filename.lastIndexOf("application")).substring(12), xyplot);
		jfreechart.removeLegend();
//                jfreechart.addSubtitle(new TextTitle("Executor: " + filename.substring(filename.lastIndexOf("application")).substring(12)));
		ChartUtilities.applyCurrentTheme(jfreechart);
		xyplot.getRenderer().setSeriesPaint(0, Color.black);
		return jfreechart;
	}

private static void writeAsPDF( JFreeChart chart, FileOutputStream out, int width, int height ) 
{ 
try 
{ 
Rectangle pagesize = new Rectangle( width, height ); 
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


	private static ArrayList<XYDataset> createDataset(String filename,
			String gcAlgorithm) {

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
			XYSeries OffHeap = new XYSeries("Spark Offheap",false,false);
			XYSeries MaxHeap = new XYSeries("Max Heap",false,false);
			//XYSeries UsedOffHeap = new XYSeries("Used OffHeap");
			XYSeries MaxPhysical = new XYSeries("Max Physical",false,false);
			XYSeries UsedCPU = new XYSeries("Used CPU",false,false);
                        XYSeries TotalMem = new XYSeries("RSS",false,false);

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
						PSOldGen.add(index, Long.valueOf(tokens[3]));
						PSYoungGen.add(
								index,
								Long.valueOf(tokens[1])
										+ Long.valueOf(tokens[2])
										);
						PermGen.add(index, Long.valueOf(tokens[4]) + Long.valueOf(tokens[0]));
						MaxHeap.add(index, Long.valueOf(tokens[7]));
						OffHeap.add(index, Long.valueOf(tokens[11]));
//OffHeap.add(index, 0L);
						UsedCPU.add(index, Math.max(0.0, Double.valueOf(tokens[14])));
                                                TotalMem.add(index, Double.valueOf(tokens[13]));
						MaxPhysical.add(index, MAX_PHYSICAL);

						UsedHeap.add(index, Long.valueOf(tokens[0]) + Long.valueOf(tokens[1]) + Long.valueOf(tokens[2]) + Long.valueOf(tokens[3]) + Long.valueOf(tokens[4]));
						if(tokens.length > 20) {
						  Storage.add(index, Long.valueOf(tokens[19]));
						  Execution.add(index, Long.valueOf(tokens[20]));
						} else {
						  Storage.add(index, 0L);
						  Execution.add(index, 0L);
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
                        //sc = new XYSeriesCollection();
                        //sc.addSeries(UsedOffHeap);
                        //xyDatasets.add(sc);
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
		if(SPARK_POOLS) {
			name += "-spark";
		}
		writeAsPDF(jfreechart, new FileOutputStream(name + ".pdf"), 640, 480);
	} catch(Exception e) {
		e.printStackTrace();
	}
		ChartPanel chartpanel = new ChartPanel(jfreechart);
		chartpanel.setMouseWheelEnabled(true);

		return chartpanel;
	}
}
