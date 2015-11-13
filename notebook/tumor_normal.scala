
object Cells {
  :remote-repo bintray-tmoerman % default % http://dl.bintray.com/tmoerman/maven % maven

  /* ... new cell ... */

  :dp org.tmoerman % vcf-comp_2.10 % 0.2.9
  :dp org.bdgenomics.adam % adam-core % 0.17.1
  - org.apache.hadoop % hadoop-client %   _
  - org.apache.spark  %     _         %   _
  - org.scala-lang    %     _         %   _
  - org.scoverage     %     _         %   _
  - joda-time         %     _         %   _

  /* ... new cell ... */

  val INC = 150
  val DEFAULT_WIDTH = 500
  val DEFAULT_HEIGHT = 300
  
  def barChart[P](data: Iterable[ProjectionCount[P]],
                  width:   Int    = DEFAULT_WIDTH,
                  height:  Int    = DEFAULT_HEIGHT,
                  x_title: String = "projection",
                  y_title: String = "count",      
                  y_axisType: String = "measure",
                  show_legend: Boolean = true) = {
    
    val addYAxis = toAxisType(y_axisType)
      
    val js = s"""
      function(data, headers, chart) {
        chart.setBounds(60, 30, $width, $height);
        
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
      }
      """
  
    DiyChart(data.toList, js, maxPoints = data.size, sizes = (width + INC, height + INC))
    
  }
  
  def stackedBarChart[P](data: Iterable[CategoryProjectionCount[P]],
                         width:   Int    = DEFAULT_WIDTH,
                         height:  Int    = DEFAULT_HEIGHT,
                         x_title: String = "projection",
                         y_title: String = "count",      
                         y_axisType: String = "measure",
                         show_legend: Boolean = true) = {
    
    val addYAxis = toAxisType(y_axisType)
      
    val js = s"""
      function(data, headers, chart) {
        chart.setBounds(60, 30, $width, $height);
        
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
      }
      """
  
    DiyChart(data.toList, js, maxPoints = data.size, sizes = (width + INC, height + INC))
  }
  
  def toAxisType(t: String) = t.toLowerCase match {
    case "category" => "addCategoryAxis"
    case "measure"  => "addMeasureAxis"
    case "log"      => "addLogAxis"
    case "pct"      => "addPctAxis"
    case _          => throw new java.lang.IllegalArgumentException(t + " illegal axis type")
  }
  
  def percentageBarChart[P](data: Iterable[CategoryProjectionCount[P]],
                            width:   Int    = DEFAULT_WIDTH,
                            height:  Int    = DEFAULT_HEIGHT,
                            x_margin: Int   = 60,
                            x_title: String = "projection",
                            y_title: String = "count",                       
                            show_legend: Boolean = true) = {
        
    val js = s"""
      function(data, headers, chart) {
        chart.setBounds($x_margin, 30, $width, $height);
        
        var y = chart.addCategoryAxis("y", ["category"]);
        y.title = "$x_title";
  
        var x = chart.addPctAxis("x", "count");
        x.title = "$y_title";
        x.tickFormat = "%";
        
        var s = chart.addSeries(["category", "projection"], dimple.plot.bar)
        s.addOrderRule("projection");
  
        if ($show_legend) {
          chart.addLegend(60, 10, $width, 20, "right");
        }
  
        chart.draw();
      }
      """
  
    DiyChart(data.toList, js, maxPoints = data.size, sizes = (width + INC, height + INC))
  }
  
  def groupedBarChart[P](data: Iterable[CategoryProjectionCount[P]],
                      width:   Int    = DEFAULT_WIDTH,
                      height:  Int    = DEFAULT_HEIGHT,
                      x_title: String = "projection",
                      y_title: String = "count",     
                      y_axisType: String = "measure",
                      show_legend: Boolean = true) = {
      
    val addYAxis = toAxisType(y_axisType)
        
    val js = s"""
      function(data, headers, chart) {
        chart.setBounds(60, 30, $width, $height);
        
        var x = chart.addCategoryAxis("x", ["category", "projection"]);
        x.title = "$x_title";
  
        var y = chart.$addYAxis("y", "count");
        y.title = "$y_title";
        y.tickFormat = ".f";
  
        var s = chart.addSeries(["category"], dimple.plot.bar)
  
        if ($show_legend) {
          chart.addLegend($width, 10, 100, 300, "right");
        }
  
        chart.draw();
      }
      """
  
    DiyChart(data.toList, js, maxPoints = data.size, sizes = (width + INC, height + INC))
  }
  
  def groupedBarChart2[P](data: Iterable[CategoryProjectionCount[P]],
                       width:  Int = DEFAULT_WIDTH,
                       height: Int = DEFAULT_HEIGHT,
                       x_title: String = "projection",
                       y_title: String = "count",     
                       y_axisType: String = "measure",
                       show_legend: Boolean = true) = {
    
    val addYAxis = toAxisType(y_axisType)
        
    val js = s"""
      function(data, headers, chart) {
        chart.setBounds(60, 30, $width, $height);
        
        var x = chart.addCategoryAxis("x", ["projection", "category"]);
        x.title = "$x_title";
  
        var y = chart.$addYAxis("y", "count");
        y.title = "$y_title";
        y.tickFormat = ".f";
  
        var s = chart.addSeries(["category"], dimple.plot.bar)
  
        if ($show_legend) {
          chart.addLegend($width, 10, 100, 300, "right");
        }
  
        chart.draw();
      }
      """
  
    DiyChart(data.toList, js, maxPoints = data.size, sizes = (width + INC, height + INC))
  }
  
  def groupedBarChart3[P](data: Iterable[CategoryProjectionCount[P]],
                       width:  Int = DEFAULT_WIDTH,
                       height: Int = DEFAULT_HEIGHT,
                       x_title: String = "projection",
                       y_title: String = "count",     
                       y_axisType: String = "measure",
                       show_legend: Boolean = true) = {
    
    val addYAxis = toAxisType(y_axisType)
        
    val js = s"""
      function(data, headers, chart) {
        chart.setBounds(60, 30, $width, $height);
        
        var x = chart.addCategoryAxis("x", ["category", "projection"]);
        x.title = "$x_title";
  
        var y = chart.$addYAxis("y", "count");
        y.title = "$y_title";
        y.tickFormat = ".f";
  
        var s = chart.addSeries(["projection"], dimple.plot.bar)
  
        if ($show_legend) {
          chart.addLegend($width, 10, 100, 300, "right");
        }
  
        chart.draw();
      }
      """
  
    DiyChart(data.toList, js, maxPoints = data.size, sizes = (width + INC, height + INC))
  }
  
  def lollipopPieChart[P](data: Iterable[CategoryProjectionCount[P]],
                       width:  Int = DEFAULT_WIDTH,
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
      }
      """
  
    DiyChart(data.toList, js, maxPoints = data.size, sizes = (width + INC, height + INC))
  }
  
  def lineChart1[P](data: Iterable[ProjectionCount[P]],
                   width:   Int    = DEFAULT_WIDTH,
                   height:  Int    = DEFAULT_HEIGHT,
                   pie_radius: Int = 20,
                   x_title: String = "projection",
                   y_title: String = "count",                 
                   x_axisType: String = "measure",
                   y_axisType: String = "measure",
                   show_legend: Boolean = true) = {
    
    val addXAxis = toAxisType(x_axisType)
    val addYAxis = toAxisType(y_axisType)
    
    val js = s"""
      function(data, headers, chart) {
        chart.setBounds(60, 30, $width, $height);
        
        var x = chart.$addXAxis("x", "projection");
        x.title = "$x_title";      
  
        var y = chart.$addYAxis("y", "count");
        y.title = "$y_title";      
                  
        var s = chart.addSeries("projection", dimple.plot.line)
        s.interpolation = "cardinal";
  
        if ($show_legend) {
          chart.addLegend($width, 10, 100, 300, "right");
        }
  
        chart.draw();
      }
      """
  
    DiyChart(data.toList, js, maxPoints = data.size, sizes = (width + INC, height + INC))
  }
  
  def lineChart[P](data: Iterable[CategoryProjectionCount[P]],
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
        if ($smooth) {
          s.interpolation = "cardinal";
        }
  
        if ($show_legend) {
          chart.addLegend($width, 10, 100, 300, "right");
        }
  
        chart.draw();
      }
      """
  
    DiyChart(data.toList, js, maxPoints = data.size, sizes = (width + INC, height + INC))
  }

  /* ... new cell ... */

  val tumorQCRDD  = sc.startQC(tumor).cache()
  val normalQCRDD = sc.startQC(normal).cache()

  /* ... new cell ... */

  tumorQCRDD.variantTypeCount

  /* ... new cell ... */

  row(barChart(tumorQCRDD.variantTypeCount, 
               y_axisType = "log", 
               y_title = "(log) count",
               x_title = "TUMOR variant types",
               width = 250, height = 200,
               show_legend = false),
      barChart(normalQCRDD.variantTypeCount, 
               y_axisType = "log", 
               y_title = "(log) count",
               x_title = "NORMAL variant types",
               width = 250, height = 200,
               show_legend = false))

  /* ... new cell ... */

  row(barChart(tumorQCRDD.multiAllelicRatio.map{case ProjectionCount(m, c) => ProjectionCount(if (m) "Multi-allelic" else "Other", c)}, 
               y_axisType = "log",
               y_title = "(log) count",
               x_title = "TUMOR",
               show_legend = false,
               width = 250, height = 200),
      barChart(normalQCRDD.multiAllelicRatio.map{case ProjectionCount(m, c) => ProjectionCount(if (m) "Multi-allelic" else "Other", c)}, 
               y_axisType = "log",
               y_title = "(log) count",
               x_title = "NORMAL",
               show_legend = false,
               width = 250, height = 200))

  /* ... new cell ... */

  row(lineChart1(tumorQCRDD.qualityDistribution(), 
                 y_axisType = "log", width = 400,  
                 x_title = "TUMOR Quality distribution"),    
      lineChart1(normalQCRDD.qualityDistribution(), 
                 y_axisType = "log", width = 400, 
                 x_title = "NORMAL Quality distribution"))

  /* ... new cell ... */

  row(lineChart1(tumorQCRDD.readDepthDistribution(), width = 400,  x_title = "TUMOR read depth distribution"),
      lineChart1(normalQCRDD.readDepthDistribution(), width = 400, x_title = "NORMAL read depth distribution"))

  /* ... new cell ... */

  import org.tmoerman.vcf.comp.VcfComparisonContext._
  import org.tmoerman.vcf.comp.core.Model._

  /* ... new cell ... */

  val wd = "/CCLE/vcf-comp/"
  
  val tumor  = wd + "4146_T.vcf.gz.annotated.gz"
  val normal = wd + "4146_N.vcf.gz.annotated.gz"

  /* ... new cell ... */

  val params = new SnpComparisonParams(labels = ("TUMOR", "NORMAL"))
  
  val snpComparison = sc.startSnpComparison(tumor, normal, params).cache()
  
  val concordantOrUnique = snpComparison.viewOnly("concordant", "unique")
  val discordant         = snpComparison.viewOnly("discordant")

  /* ... new cell ... */

  groupedBarChart3(concordantOrUnique.baseChangeCount, x_title = "base change") ++
  groupedBarChart3(discordant.baseChangeCount, x_title = "base change")

  /* ... new cell ... */

  groupedBarChart2(concordantOrUnique.baseChangeCount, x_title = "base change") ++
  groupedBarChart2(discordant.baseChangeCount, x_title = "base change")

  /* ... new cell ... */

  groupedBarChart2(concordantOrUnique.baseChangePatternCount, x_title = "base change pattern") ++
  groupedBarChart2(discordant.baseChangePatternCount, x_title = "base change pattern")

  /* ... new cell ... */

  lollipopPieChart(snpComparison.zygosityCount,                  
                   pie_radius = 30,
                   x_title = "Categories", 
                   y_logAxis = false,
                   y_title = "SNP count -- zygosity ratio")

  /* ... new cell ... */

  percentageBarChart(snpComparison.zygosityCount,                   
                     x_margin = 120,
                     x_title = "Categories",
                     y_title = "Zygosity ratio")

  /* ... new cell ... */

  lollipopPieChart(snpComparison.baseChangeTypeCount,                  
                   pie_radius = 30,
                   x_title = "Categories", 
                   y_logAxis = false,
                   y_title = "SNP count -- Ti / Tv ratio")

  /* ... new cell ... */

  percentageBarChart(snpComparison.baseChangeTypeCount,                                        
                     height = 250,
                     x_margin = 120,
                     x_title = "Categories",
                     y_title = "Ti / Tv ratio")

  /* ... new cell ... */

  groupedBarChart2(snpComparison.functionalAnnotationCount,
                   width = 900,                                  
                   x_title = "Functional impact",
                   y_axisType = "log",
                   y_title = "Count") 

  /* ... new cell ... */

  groupedBarChart2(snpComparison.transcriptBiotypeCount,
                   width = 900,                                  
                   x_title = "Transcript biotype",
                   y_axisType = "log",
                   y_title = "Count") 

  /* ... new cell ... */

  groupedBarChart2(snpComparison.functionalImpactCount,                                   
                   x_title = "Functional impact",
                   y_axisType = "log",
                   y_title = "Log count") 

  /* ... new cell ... */

  lineChart(concordantOrUnique.readDepthDistribution(),
            width = 900,
            x_title = "read depth") ++
  lineChart(concordantOrUnique.readDepthDistribution(quantize(5)),
            width = 900,
            x_title = "read depth",
            smooth = true)

  /* ... new cell ... */

  lineChart(concordantOrUnique.qualityDistribution(),          
            x_title = "Quality",
           width = 900) ++
  lineChart(concordantOrUnique.qualityDistribution(quantize(100)),
            x_title = "Quality",
            x_min = 100d,
            x_axisType = "log")

  /* ... new cell ... */

  lineChart(concordantOrUnique.qualityDistribution(quantize(250)),
            x_title = "Quality (quantized by 250)")

  /* ... new cell ... */

  lineChart(concordantOrUnique.alleleFrequencyDistribution(quantize(.025)),        
            x_title = "Allele frequency distribution",
            width = 600,
            height = 300,
            y_title = "Count") ++
  lineChart(concordantOrUnique.alleleFrequencyDistribution(quantize(.1)),
            x_title = "Allele frequency",
            y_axisType = "measure")

  /* ... new cell ... */

  groupedBarChart(snpComparison.clinvarRatio,                
                  height = 250,
                  x_title = "Clinvar annotated",
                  y_axisType = "log") ++
  groupedBarChart2(snpComparison.clinvarRatio,
                   x_title = "Clinvar annotated",
                   y_axisType = "log")

  /* ... new cell ... */

  groupedBarChart2(concordantOrUnique.commonSnpRatio,
                   x_title = "Common SNPs (DbSNP annotated)",
                   y_axisType = "log")

  /* ... new cell ... */
}
              