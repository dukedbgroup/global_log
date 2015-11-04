// Decompiled by Jad v1.5.8e2. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://kpdus.tripod.com/jad.html
// Decompiler options: packimports(3) fieldsfirst ansi space 
package edu.duke.globallog.sparklogprocessor;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;

import javax.swing.JPanel;

import org.jfree.chart.*;
import org.jfree.chart.axis.*;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.StandardXYItemRenderer;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.title.TextTitle;
import org.jfree.data.time.*;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

public class ApplicationHeapUsagePlotter extends ApplicationFrame {

	
	public static void main(String args[]) {
		
		
		String applicationID = args[0]; //"application_1446265188032_0144";
		String testName = "test63";
		
		ApplicationHeapUsagePlotter multipleaxisdemo1 = new ApplicationHeapUsagePlotter(
				testName, applicationID,
				applicationID);
		multipleaxisdemo1.pack();
		RefineryUtilities.centerFrameOnScreen(multipleaxisdemo1);
		multipleaxisdemo1.setVisible(true);
	}

	public ApplicationHeapUsagePlotter(String testName, String applicationID,
			String s) {
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
								.getAbsolutePath());
				chartpanel.setPreferredSize(new Dimension(600, 270));
				chartpanel.setDomainZoomable(true);
				chartpanel.setRangeZoomable(true);

				// setContentPane(chartpanel);
				add(chartpanel);
			}
		}
	}

	private static JFreeChart createChart(String executorID, String filename) {
		ArrayList<XYDataset> xydatasets = createDataset(filename);
		JFreeChart jfreechart = ChartFactory.createXYLineChart("Heap Usage",
				"Sample index", "Heap (Byte)", xydatasets.get(0),
				PlotOrientation.VERTICAL, true, true, false);
		jfreechart.addSubtitle(new TextTitle("Executor " + executorID));
		XYPlot xyplot = (XYPlot) jfreechart.getPlot();
		xyplot.setOrientation(PlotOrientation.VERTICAL);
		xyplot.setDomainPannable(true);
		xyplot.setRangePannable(true);
		// xyplot.getRangeAxis().setFixedDimension(15D);
		// NumberAxis numberaxis = new NumberAxis("Range Axis 2");
		// numberaxis.setFixedDimension(10D);
		// numberaxis.setAutoRangeIncludesZero(false);
		// xyplot.setRangeAxis(1, numberaxis);
		// xyplot.setRangeAxisLocation(1, AxisLocation.BOTTOM_OR_LEFT);

		for (int index = 1; index <= xydatasets.size() - 1; index++) {

			XYDataset xydataset = xydatasets.get(index);
			xyplot.setDataset(index, xydataset);
			// xyplot.mapDatasetToRangeAxis(1, 1);
			StandardXYItemRenderer standardxyitemrenderer = new StandardXYItemRenderer();
			xyplot.setRenderer(index, standardxyitemrenderer);
		}

		ChartUtilities.applyCurrentTheme(jfreechart);
		xyplot.getRenderer().setSeriesPaint(0, Color.black);
		return jfreechart;
	}

	private static ArrayList<XYDataset> createDataset(String filename) {

		XYSeries G1OldGen = new XYSeries("G1 Old Gen");
		XYSeries G1YoungGen = new XYSeries("G1 Young Gen");
		XYSeries UsedHeap = new XYSeries("Used Heap");
		XYSeries CommittedHeap = new XYSeries("Committed Heap");

		String line;
		long index = 1;
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new FileInputStream(filename), Charset.forName("UTF-8")));
			while ((line = br.readLine()) != null) {
				// Deal with the line
				if (!line.contains("application") && !line.contains("Code")) {
					String tokens[] = line.split("\t");
					G1OldGen.add(index, Long.valueOf(tokens[3]));
					G1YoungGen.add(index,
							Long.valueOf(tokens[1]) + Long.valueOf(tokens[2])
									+ Long.valueOf(tokens[3]));
					UsedHeap.add(index, Long.valueOf(tokens[5]));
					CommittedHeap.add(index, Long.valueOf(tokens[6]));
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
		return xyDatasets;
	}

	public static JPanel createDemoPanel(String executorID, String filename) {
		JFreeChart jfreechart = createChart(executorID, filename);
		ChartPanel chartpanel = new ChartPanel(jfreechart);
		chartpanel.setMouseWheelEnabled(true);

		return chartpanel;
	}
}
