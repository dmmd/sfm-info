package edu.nyu.dlts.nlpTools

import java.io.File
import java.io.FileInputStream
import java.sql.{DriverManager, Connection, Statement, PreparedStatement, ResultSet}
import scala.collection.JavaConversions._
import com.typesafe.config._



class TokenEnts(){
  
  import opennlp.tools.namefind.NameFinderME
  import opennlp.tools.namefind.TokenNameFinderModel
  import opennlp.tools.tokenize.SimpleTokenizer
  import opennlp.tools.tokenize.Tokenizer
  import opennlp.tools.util.Span
  
  val tokenizer = SimpleTokenizer.INSTANCE
  val locFinder = getFinder("src/main/resources/en-ner-location.bin")

  def getFinder(model: String): NameFinderME = {new NameFinderME(new TokenNameFinderModel(new FileInputStream(new File(model))))}
  
  def getEnts(data: String, finder: NameFinderME): java.util.HashSet[String] = {
    val ents = new java.util.HashSet[String]
    val tokenSpans = tokenizer.tokenizePos(data)
    val tokens = Span.spansToStrings(tokenSpans, data)
    val locs = finder.find(tokens)
    val probs = finder.probs(locs)
    var count = 0
    for(loc <- locs){
      if(probs(count) > 0.8){
      val startSpan = tokenSpans(loc.getStart())
      val nameStart = startSpan.getStart()
      val endSpan = tokenSpans(loc.getEnd() -1)
      val nameEnd = endSpan.getEnd()
      ents.add(data.substring(nameStart, nameEnd))
      }
      count += 1
    } 
    ents
  }
}

class Db(){
  
  val conf = ConfigFactory.load()
  val con = DriverManager.getConnection(conf.getString("sfminfo.dbUrl") + ":5433/sfm?user=sfm&password=sfm")
  val tokenEnts = new TokenEnts()  
  
  def getName(id: Int): String = {
    val statement = con.createStatement()
    val rs = statement.executeQuery("SELECT name FROM ui_twitteruser WHERE id = " + id)
    rs.next()
    val name = rs.getString(1)
    statement.close
    name
  }
  
  def getUserIds(): java.util.ArrayList[Int] = {
    val ids = new java.util.ArrayList[Int]
    val statement = con.createStatement()
    val rs = statement.executeQuery("Select id FROM ui_twitteruser")
    while(rs.next()){ids.add(rs.getInt(1))}
    statement.close
    ids
  }

  def getLocs(id: Integer): java.util.TreeMap[String, Int] = {
    val map = new java.util.TreeMap[String, Int]
    val ps: PreparedStatement = con.prepareStatement("SELECT item_text FROM ui_twitteruseritem WHERE twitter_user_id = ?")
    ps.setInt(1, id)
    val rs = ps.executeQuery()
    while(rs.next()){
      val ents = tokenEnts.getEnts(rs.getString(1), tokenEnts.locFinder)
      for(ent <- ents){
	if (map.contains(ent)){
	  map.put(ent, map.get(ent) + 1)}
	else{
	  map.put(ent, 1)
	}
      }
    }
    ps.close
    map
  }
}

object Run{
  val db = new Db()
  def main(args: Array[String]){ 
    for(id <- db.getUserIds()){
      println(db.getName(id))
      db.getLocs(id).foreach{kv =>
	println("\t" + kv._1 + " [" + kv._2 + "]")
      } 
    }
  }
}
