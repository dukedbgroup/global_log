// Decompiled by Jad v1.5.8e2. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://kpdus.tripod.com/jad.html
// Decompiler options: packimports(3) fieldsfirst ansi space 

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

		String applicationID = "application_1446265188032_0241";
		String testName = "test64";
//		String gcAlgorithm = "G1";
		String gcAlgorithm = "Parallel GC";
		boolean showUsedCPU = true;

		ApplicationHeapUsagePlotter multipleaxisdemo1 = new ApplicationHeapUsagePlotter(
				testName, applicationID, applicationID, gcAlgorithm, showUsedCPU);
		multipleaxisdemo1.pack();
		RefineryUtilities.centerFrameOnScreen(multipleaxisdemo1);
		multipleaxisdemo1.setVisible(true);
	}

	public ApplicationHeapUsagePlotter(String testName, String applicationID,
			String s, String gcAlgorithm, boolean showUsedCPU) {
		super(s);
		this.setLayout(new GridLayout(2, 5));

		File basedir = new File("/home/yuzhanghan1982/2015summer/results/"
				+ testName + "/" + applicationID);
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

	private static JFreeChart createChart(String executorID, String filename,
			String gcAlgorithm, boolean showUsedCPU) {
		ArrayList<XYDataset> xydatasets = createDataset(filename, gcAlgorithm);
		JFreeChart jfreechart = ChartFactory.createXYLineChart("Heap Usage",
				"Sample index", "Heap (Byte)", xydatasets.get(0),
				PlotOrientation.VERTICAL, true, true, false);
		jfreechart.addSubtitle(new TextTitle("Executor " + executorID));
		XYPlot xyplot = (XYPlot) jfreechart.getPlot();
		xyplot.setOrientation(PlotOrientation.VERTICAL);
		xyplot.setDomainPannable(true);
		xyplot.setRangePannable(true);

		XYDataset xydataset;
		int index;
		for (index = 1; index <= xydatasets.size() - 2; index++) {

			xydataset = xydatasets.get(index);
			xyplot.setDataset(index, xydataset);
			// xyplot.mapDatasetToRangeAxis(1, 1);
			StandardXYItemRenderer standardxyitemrenderer = new StandardXYItemRenderer();
			xyplot.setRenderer(index, standardxyitemrenderer);
		}

		if (showUsedCPU) {
			NumberAxis axis2 = new NumberAxis("Used CPU");
			// axis2.setFixedDimension(10.0);
			axis2.setAutoRangeIncludesZero(false);
			// axis2.setLabelPaint(Color.red);
			// axis2.setTickLabelPaint(Color.red);
			xyplot.setRangeAxis(index, axis2);
			// xyplot.setRangeAxisLocation(1, AxisLocation.BOTTOM_OR_LEFT);

			xyplot.setDataset(index, xydatasets.get(index));
			xyplot.mapDatasetToRangeAxis(index, index);
			XYItemRenderer renderer2 = new StandardXYItemRenderer();
			// renderer2.setSeriesPaint(0, Color.red);
			 ;
			xyplot.setRenderer(index, renderer2);
		}
		ChartUtilities.applyCurrentTheme(jfreechart);
		xyplot.getRenderer().setSeriesPaint(0, Color.black);
		return jfreechart;
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
			XYSeries PSOldGen = new XYSeries("PS Old Gen");
			XYSeries PSYoungGen = new XYSeries("PS Young Gen");
			XYSeries UsedHeap = new XYSeries("Used Heap");
			XYSeries MaxHeap = new XYSeries("Max Heap");
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
						PSOldGen.add(index, Long.valueOf(tokens[3]));
						PSYoungGen.add(
								index,
								Long.valueOf(tokens[1])
										+ Long.valueOf(tokens[2])
										+ Long.valueOf(tokens[3]));
						UsedHeap.add(index, Long.valueOf(tokens[5]));
						MaxHeap.add(index, Long.valueOf(tokens[7]));
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
			sc.addSeries(PSOldGen);
			xyDatasets.add(sc);
			sc = new XYSeriesCollection();
			sc.addSeries(PSYoungGen);
			xyDatasets.add(sc);
			sc = new XYSeriesCollection();
			sc.addSeries(UsedHeap);
			xyDatasets.add(sc);
			sc = new XYSeriesCollection();
			sc.addSeries(MaxHeap);
			xyDatasets.add(sc);
			sc = new XYSeriesCollection();
			sc.addSeries(UsedCPU);
			xyDatasets.add(sc);
			return xyDatasets;
		}

		return null;
	}

	public static JPanel createDemoPanel(String executorID, String filename,
			String gcAlgorithm, boolean showUsedCPU) {
		JFreeChart jfreechart = createChart(executorID, filename, gcAlgorithm, showUsedCPU);
		ChartPanel chartpanel = new ChartPanel(jfreechart);
		chartpanel.setMouseWheelEnabled(true);

		return chartpanel;
	}
}

