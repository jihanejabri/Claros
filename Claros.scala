package Claros

object  example extends App {

  val trip=sc.textFile("C:/Users/Jabri/Desktop/RDF/Claros.csv").map(x=>x.split(" "))
  val.count//37
  val ClarosE=sc.textFile("C:/Users/Jabri/Desktop/RDF/Claros_L+eqr.dlog.txt").map(x=>x.split(" ")).map(x=>(x(1),(x(0),x(2))))
  //1343
  ClarosEq.count
  val ClarosEq=sc.textFile("C:/Users/Jabri/Desktop/RDF/Claros_L+eq.dlog.txt").map(x=>x.split(" "))//1343
  ClarosEq.count

  val trip2 = ClarosEq.filter(x=>x.length()>0)
  trip2.count//1342


  def lireT(file: String): RDD[(Long, Long, Int)] = {
    return sc.textFile(file).
      map(l => l.substring(1, l.length -1).split(",")).
      map(t => (t(0).toLong, t(1).toLong, (t(0).toInt)))
  }

    val triplesE = lireT(ClarosE)
    triplesE.persist//31

    triplesE.count
  
    val triplesEq = lireT(ClarosEq)
    triplesEq.persist//31

    triplesEq.count

  def filterPropreties(file: RDD[(Long, Long, Int)], prop: Long): RDD[(Long, Long, Int)] = {
    return file.filter(x => x._2 == prop).map{ case (s,p,o,f) => (s,o,f)}
  }

  val TripOwl1 = filterProperties(triplesE)//70
  val TripOwl2 = filterProperties(triplesEq)//82
  

  val l = TripOwl1.map(x=>x.length()).reduce((a,b)=>a.max(b))
  val t = TripOwl1.map(x=>x.length()).reduce((a,b)=>a.min(b))
  
  val l = TripOwl2.map(x=>x.length()).reduce((a,b)=>a.max(b))
  val t = TripOwl2.map(x=>x.length()).reduce((a,b)=>a.min(b))


}
