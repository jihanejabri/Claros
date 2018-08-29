package Claros

object  example extends App {

  val trip=sc.textFile("C:/Users/Jabri/Desktop/RDF/Claros.csv").map(x=>x.split(" "))
  val.count

  def lireTriples(input: String): RDD[(Long, Long, Long, Int)] = {
    return sc.textFile(input).
      map(line => line.substring(1, line.length -1).split(",")).
      map(tab => (tab(0).toLong, tab(1).toLong, tab(2).toLong, (tab(0).toInt)
  }

    val triples = lireTriples(trip)
    triples.persist

    triples.count

  def filterProp(input: RDD[(Long, Long, Long, Int)], prop: Long): RDD[(Long, Long, Int)] = {
    x  // on projette sur S et O car P est fixe
    return input.filter(x => x._2 == prop).map{ case (s,p,o,f) => (s,o,f)}
  }

  val trip2 = trip.filter(x=>x.length()>0)
  trip2.count

  val l = trip2.map(x=>x.length()).reduce((a,b)=>a.max(b))

  val t = trip2.map(x=>x.length()).reduce((a,b)=>a.min(b))


}
