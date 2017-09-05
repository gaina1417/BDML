package works.gaina.bda.app

import breeze.numerics.sin
import org.jfree.chart._
import org.jfree.data.category.CategoryDataset
import org.jfree.data.xy
import org.jfree.data.xy._

object JFreeChartDemo extends App {
  val x = Array[Double](0.0,0.45,0.90,1.35,1.80,2.25,2.70,3.15,3.60)
  val y = x.map(sin(_))
  val dataset = new DefaultXYDataset
//  val dataset = new CategoryTableXYDataset
  dataset.addSeries("Series 1",Array(x,y))

  val frame = new ChartFrame(
    "Title",
//    ChartFactory.createLineChart(
    ChartFactory.createScatterPlot(
      "Plot",
      "X Label",
      "Y Label",
      dataset,
      org.jfree.chart.plot.PlotOrientation.VERTICAL,
      false,false,false
    )
  )
  frame.pack()
  frame.setVisible(true)
}

/*
createHistogram(java.lang.String title,
                java.lang.String xAxisLabel, java.lang.String yAxisLabel,
                IntervalXYDataset dataset, PlotOrientation orientation,
                boolean legend, boolean tooltips, boolean urls)

createScatterPlot(java.lang.String title,
                java.lang.String xAxisLabel, java.lang.String yAxisLabel,
                XYDataset dataset, PlotOrientation orientation,
                boolean legend, boolean tooltips, boolean urls)

 */