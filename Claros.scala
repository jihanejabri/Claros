package Claros

object  example extends App {

  val trip=sc.textFile("C:/Users/Jabri/Desktop/RDF/Claros.csv").map(x=>x.split(" "))
  val.count
  val ClarosE=sc.textFile("C:/Users/Jabri/Desktop/RDF/Claros_L+eqr.dlog.txt").map(x=>x.split(" ")).map(x=>(x(1),(x(0),x(2))))
  //1343
  ClarosEqr.count
  val ClarosEq=sc.textFile("C:/Users/Jabri/Desktop/RDF/Claros_L+eq.dlog.txt").map(x=>x.split(" "))//1343
  ClarosEq.count

  val trip2 = ClarosEqr.filter(x=>x.length()>0)
  trip2.count//1342


  def lireT(input: String): RDD[(Long, Long, Int)] = {
    return sc.textFile(input).
      map(l => l.substring(1, line.length -1).split(",")).
      map(t => (t(0).toLong, t(1).toLong, (t(0).toInt)))
  }

    val triples = lireT(trip)
    triples.persist//31

    triples.count

  def filterPropreties(input: RDD[(Long, Long, Int)], prop: Long): RDD[(Long, Long, Int)] = {
    return input.filter(x => x._2 == prop).map{ case (s,p,o,f) => (s,o,f)}
  }



  val l = trip2.map(x=>x.length()).reduce((a,b)=>a.max(b))//145

  val t = trip2.map(x=>x.length()).reduce((a,b)=>a.min(b))


}
