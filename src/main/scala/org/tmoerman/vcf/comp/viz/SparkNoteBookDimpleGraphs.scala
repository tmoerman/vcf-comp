package org.tmoerman.vcf.comp.viz

import org.tmoerman.vcf.comp.core.Model.{CategoryCount, ProjectionCount, CategoryProjectionCount}
import org.tmoerman.vcf.comp.util.ApiHelp

/**
  * Created by tmo on 22/11/15.
  */
object SparkNoteBookDimpleGraphs {

  val MARGIN_INC     = 150
  val DEFAULT_WIDTH  = 500
  val DEFAULT_HEIGHT = 300

  def toAxisType(t: String) = t.toLowerCase match {
    case "category" => "addCategoryAxis"
    case "measure"  => "addMeasureAxis"
    case "log"      => "addLogAxis"
    case "pct"      => "addPctAxis"
    case _          => throw new java.lang.IllegalArgumentException(t + " illegal axis type")
  }

  implicit def pimpCategoryCount(data: Iterable[CategoryCount]): CategoryCountDimpleChartFunctions =
    new CategoryCountDimpleChartFunctions(data)

  implicit def pimpProjectionCount[P](data: Iterable[ProjectionCount[P]]): ProjectionCountDimpleChartFunctions[P] =
    new ProjectionCountDimpleChartFunctions[P](data)

  implicit def pimpCategoryProjectionCount[P](data: Iterable[CategoryProjectionCount[P]]): CategoryProjectionCountDimpleGraphFunctions[P] =
    new CategoryProjectionCountDimpleGraphFunctions[P](data)

  case class DimpleChart(data: List[Any],
                         js: String,
                         sizes: (Int, Int)) {

    lazy val maxPoints = data.size

    override def toString = getClass.getSimpleName

  }

}

class CategoryCountDimpleChartFunctions(private[this] val data: Iterable[CategoryCount]) extends Serializable with ApiHelp {
  import SparkNoteBookDimpleGraphs._

  def barChart(width:   Int    = DEFAULT_WIDTH,
               height:  Int    = DEFAULT_HEIGHT,
               x_margin: Int   = 60,
               x_title: String = "category",
               y_title: String = "count",
               y_axisType: String = "measure",
               show_legend: Boolean = true) = {

    val addYAxis = toAxisType(y_axisType)

    val js = s"""
    function(data, headers, chart) {
      chart.setBounds($x_margin, 30, $width, $height);

      var x = chart.addCategoryAxis("x", "category");
      x.title = "$x_title";

      var y = chart.$addYAxis("y", "count");
      y.title = "$y_title";
      y.tickFormat = ".f";

      var s = chart.addSeries("", dimple.plot.bar)
      s.addOrderRule("category");

      if ($show_legend) {
        chart.addLegend(60, 10, $width, 20, "right");
      }

      chart.draw();
    }"""

    DimpleChart(data.toList, js, sizes = (width + MARGIN_INC, height + MARGIN_INC))
  }

  def table = data.toList

}

class ProjectionCountDimpleChartFunctions[P](private[this] val data: Iterable[ProjectionCount[P]]) extends Serializable with ApiHelp {

  import SparkNoteBookDimpleGraphs._

  def barChart(width:   Int    = DEFAULT_WIDTH,
               height:  Int    = DEFAULT_HEIGHT,
               x_margin: Int   = 60,
               x_title: String = "projection",
               y_title: String = "count",
               y_axisType: String = "measure",
               show_legend: Boolean = true) = {

    val addYAxis = toAxisType(y_axisType)

    val js = s"""
    function(data, headers, chart) {
      chart.setBounds($x_margin, 30, $width, $height);

      var x = chart.addCategoryAxis("x", "projection");
      x.title = "$x_title";

      var y = chart.$addYAxis("y", "count");
      y.title = "$y_title";
      y.tickFormat = ".f";

      var s = chart.addSeries("", dimple.plot.bar)

      if ($show_legend) {
        chart.addLegend(60, 10, $width, 20, "right");
      }

      chart.draw();
    }"""

    DimpleChart(data.toList, js, sizes = (width + MARGIN_INC, height + MARGIN_INC))
  }

  def table = data.toList

}

class CategoryProjectionCountDimpleGraphFunctions[P](private[this] val data: Iterable[CategoryProjectionCount[P]]) extends Serializable with ApiHelp {
  import SparkNoteBookDimpleGraphs._

  def stackedBarChart(width:   Int    = DEFAULT_WIDTH,
                      height:  Int    = DEFAULT_HEIGHT,
                      x_margin: Int   = 60,
                      x_title: String = "projection",
                      y_title: String = "count",
                      y_axisType: String = "measure",
                      show_legend: Boolean = true) = {

    val addYAxis = toAxisType(y_axisType)

    val js = s"""
    function(data, headers, chart) {
      chart.setBounds($x_margin, 30, $width, $height);

      var x = chart.addCategoryAxis("x", "projection");
      x.title = "$x_title";

      var y = chart.$addYAxis("y", "count");
      y.title = "$y_title";
      y.tickFormat = ".f";

      var s = chart.addSeries(["category"], dimple.plot.bar)

      if ($show_legend) {
        chart.addLegend(60, 10, $width, 20, "right");
      }

      chart.draw();
    }"""

    DimpleChart(data.toList, js, sizes = (width + MARGIN_INC, height + MARGIN_INC))
  }

  def percentageBarChart(width:    Int   = DEFAULT_WIDTH,
                         height:   Int   = DEFAULT_HEIGHT,
                         x_margin: Int   = 60,
                         x_title: String = "projection",
                         y_title: String = "count",
                         show_legend: Boolean = true) = {

    val js = s"""
    function(data, headers, chart) {
      chart.setBounds($x_margin, 30, $width, $height);

      var x = chart.addPctAxis("x", "count");
      x.title = "$y_title";
      x.tickFormat = "%";

      var y = chart.addCategoryAxis("y", ["category"]);
      y.title = "$x_title";

      var s = chart.addSeries(["category", "projection"], dimple.plot.bar)
      s.addOrderRule("projection");

      if ($show_legend) {
        chart.addLegend(60, 10, $width, 20, "right");
      }

      chart.draw();
    }"""

    DimpleChart(data.toList, js, sizes = (width + MARGIN_INC, height + MARGIN_INC))
  }

  def groupedBarChart(width:   Int    = DEFAULT_WIDTH,
                      height:  Int    = DEFAULT_HEIGHT,
                      x_title: String = "projection",
                      x_reverse: Boolean = false,
                      y_title: String = "count",
                      y_axisType: String = "measure",
                      show_legend: Boolean = true) = {

    val addYAxis = toAxisType(y_axisType)

    val categoryFields = if (x_reverse)
      """["category", "projection"]""" else
      """["projection", "category"]"""

    val js = s"""
    function(data, headers, chart) {
      chart.setBounds(60, 30, $width, $height);

      var x = chart.addCategoryAxis("x", $categoryFields);
      x.title = "$x_title";

      var y = chart.$addYAxis("y", "count");
      y.title = "$y_title";
      y.tickFormat = ".f";

      var s = chart.addSeries(["category"], dimple.plot.bar)
      s.addOrderRule("category");

      if ($show_legend) {
        chart.addLegend($width, 10, 100, 300, "right");
      }

      chart.draw();
    }"""

    DimpleChart(data.toList, js, sizes = (width + MARGIN_INC, height + MARGIN_INC))
  }

  def lollipopPieChart(width:  Int = DEFAULT_WIDTH,
                       height: Int = DEFAULT_HEIGHT,
                       pie_radius: Int = 20,
                       x_title: String = "projection",
                       y_title: String = "count",
                       y_logAxis: Boolean = false,
                       show_legend: Boolean = true) = {

    val addYAxis = if (y_logAxis) "addLogAxis" else "addMeasureAxis"

    val js = s"""
    function(data, headers, chart) {
      chart.setBounds(60, 30, $width, $height);

      var x = chart.addCategoryAxis("x", ["category"]);
      x.title = "$x_title";

      var y = chart.$addYAxis("y", "count");
      y.title = "$y_title";
      y.tickFormat = ".f";

      var p = chart.addMeasureAxis("p", "count");

      var s = chart.addSeries(["category", "projection"], dimple.plot.pie)
      s.radius = $pie_radius
      s.addOrderRule("projection");

      if ($show_legend) {
        chart.addLegend($width, 10, 100, 300, "right");
      }

      chart.draw();
    }"""

    DimpleChart(data.toList, js, sizes = (width + MARGIN_INC, height + MARGIN_INC))
  }

  def lineChart(data: Iterable[CategoryProjectionCount[P]],
                width:   Int    = DEFAULT_WIDTH,
                height:  Int    = DEFAULT_HEIGHT,
                pie_radius: Int = 20,
                x_title: String = "projection",
                y_title: String = "count",
                x_min:   Double = 0,
                y_min:   Double = 0,
                smooth: Boolean = false,
                x_axisType: String = "measure",
                y_axisType: String = "measure",
                show_legend: Boolean = true) = {

    val addXAxis = toAxisType(x_axisType)
    val addYAxis = toAxisType(y_axisType)

    val js = s"""
    function(data, headers, chart) {
      chart.setBounds(60, 30, $width, $height);

      var x = chart.$addXAxis("x", ["projection"]);
      x.title = "$x_title";
      x.overrideMin = $x_min;

      var y = chart.$addYAxis("y", "count");
      y.title = "$y_title";
      y.overrideMin = $y_min;

      var s = chart.addSeries(["projection", "category"], dimple.plot.line)
      s.addOrderRule("category");
      if ($smooth) {
        s.interpolation = "cardinal";
      }

      if ($show_legend) {
        chart.addLegend($width, 10, 100, 300, "right");
      }

      chart.draw();
    }
    """

    DimpleChart(data.toList, js, sizes = (width + MARGIN_INC, height + MARGIN_INC))
  }

  def table = data.toList

}