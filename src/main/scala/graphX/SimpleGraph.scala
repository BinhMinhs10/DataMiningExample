//package graphX
//
//import org.apache.spark._
//import org.apache.spark.graphx._
//import org.apache.spark.rdd.RDD
//class Graph[VD, ED]{
//  val vertices: VertexRDD[VD]
//  val edges: EdgeRDD[ED]
//}
//
//object SimpleGraph {
//
//  def main(args: Array[String]): Unit = {
//
//    class VertexProperty()
//    case class UserProperty(val name: String) extends VertexProperty
//    case class ProductProperty(val name: String, val price: Double) extends VertexProperty
//    // The graph might then have the type:
//    var graph: Graph[VertexProperty, String] = null
//
//
//  }
//}
