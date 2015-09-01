object charts {
  import sys.process._
  import com.redhat.et.silex.util.OptionalArg
  import OptionalArg.fullOptionSupport
  import io.continuum.bokeh._
  import org.json4s.JsonDSL._
  import org.json4s.jackson.JsonMethods._
  import com.freevariable.firkin

  private def defaultTools(plot: Plot) = List(
    new PanTool().plot(plot),
    new BoxZoomTool().plot(plot),
    new ResetTool().plot(plot),
    new PreviewSaveTool().plot(plot))


  def scatter[N :Numeric](xdata: Seq[N], ydata: Seq[N],
    title: OptionalArg[String] = None,
    xlab: OptionalArg[String] = None,
    ylab: OptionalArg[String] = None,
    logx: Boolean = false,
    logy: Boolean = false): Plot = {

    val num = implicitly[Numeric[N]]

    val xdat = xdata.map(t => num.toDouble(t))
    val ydat = ydata.map(t => num.toDouble(t))

    object source extends ColumnDataSource {
      val x = column(xdat)
      val y = column(ydat)
    }

    val circle = new Circle()
      .x(source.x)
      .y(source.y)
      .size(10)
      .line_color(Color.Black)

    val renderer = new GlyphRenderer()
      .data_source(source)
      .glyph(circle)

    val plot = new Plot()

    plot.x_range := new DataRange1d()
    plot.y_range := new DataRange1d()
    title.foreach { plot.title := _ }

    // to-do: log axis seems to be ignored
    val xaxis = if (logx) new LogAxis() else new LinearAxis()
    val yaxis = if (logy) new LogAxis() else new LinearAxis()
    xaxis.plot := plot
    xaxis.bounds := ((xdat.min, xdat.max))
    xaxis.major_tick_in := 0
    xlab.foreach { xaxis.axis_label := _ }

    yaxis.plot := plot
    yaxis.bounds := ((ydat.min, ydat.max))
    yaxis.major_tick_in := 0
    ylab.foreach { yaxis.axis_label := _ }

    plot.below <<= (xaxis :: _)
    plot.left <<= (yaxis :: _)

    val xgrid = new Grid().plot(plot).axis(xaxis).dimension(0)
    val ygrid = new Grid().plot(plot).axis(yaxis).dimension(1)

    plot.renderers := List(xaxis, yaxis, xgrid, ygrid, renderer)
    plot.tools := defaultTools(plot)
    plot
  }

  def save(plot: Plot, fname: String) {
    val doc = new Document(plot)
    doc.save(fname)
  }

  def startServer() {
    com.freevariable.firkin.Firkin.start()(colossus.IOSystem())
  }

  class LinePlot(tag: String, client: firkin.Client) {
    var x: Option[List[Double]] = None
    var y: Option[List[Double]] = None
    def x_=(xNew: Seq[Double]) {
      x = Some(xNew.toList)
    }
    def y_=(yNew: Seq[Double]) {
      y = Some(yNew.toList)
    }
    def plot() {
      val json = for (
        xdata <- x;
        ydata <- y
      ) yield {
        ("x" -> xdata) ~ ("y" -> ydata)
      }

      json.foreach { j => client.publish(tag, compact(render(j))) }
    }
  }

  def line(tag: String = "line", html: OptionalArg[String] = None) = {
    val cmd = s"""/home/eje/bin/bokeh_plot line $tag ${html.getOrElse(s"/tmp/line_$tag")}.html"""
    //println(s"cmd= $cmd")
    cmd.!
    val client = new firkin.Client("localhost", 4091)
    new LinePlot(tag, client)
  }

  
}
