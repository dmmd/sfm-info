package edu.nyu.dlts.nlpTools

import java.io.File
import java.io.FileInputStream
import java.sql.{DriverManager, Connection, Statement, ResultSet}
import scala.collection.JavaConversions._

class TokenEnts(){
  
  import opennlp.tools.namefind.NameFinderME
  import opennlp.tools.namefind.TokenNameFinderModel
  import opennlp.tools.tokenize.SimpleTokenizer
  import opennlp.tools.tokenize.Tokenizer
  import opennlp.tools.util.Span

  val model = new File("src/main/resources/en-ner-location.bin")
  val tokenizer = SimpleTokenizer.INSTANCE
  val finder = new NameFinderME(new TokenNameFinderModel(new FileInputStream(model)))
  
  
  def getEnts(data: String): java.util.HashSet[String] = {
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

  val con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/sfm?user=sfm&password=sfm")
  
  def getName(id: Int): String = {
    val statement = con.createStatement()
    val rs = statement.executeQuery("SELECT name FROM ui_twitteruser WHERE id = " + id)
    rs.next()
    rs.getString(1)
  }
}

object Process{
  val entMap = new java.util.TreeMap[Int, java.util.TreeMap[String, Int]]
  val db = new Db()
  
def main(args: Array[String]){ 
    val tokenEnts = new TokenEnts()
    val statement = db.con.createStatement()
    val rs = statement.executeQuery("SELECT item_text, twitter_user_id FROM ui_twitteruseritem")
    
    while(rs.next()){
      val id = rs.getInt(2)
      val map = getMap(id)
      
      def ents = tokenEnts.getEnts(rs.getString(1))
      for(ent <- ents){
	if(map.contains(ent)){
	  map.put(ent, map.get(ent) + 1)
	} else {
	  map.put(ent, 1)
	}
      }
      entMap.put(id, map)
    }
    
    entMap.foreach(ent => 
      printMap(ent._1, ent._2)
    )
  }

  def printMap(id: Int, emap: java.util.TreeMap[String, Int]): Unit = {
    println(id + ": " + db.getName(id))
    emap.foreach(kv => println("\t" + kv._1 + " [" + kv._2 + "]"))
  }

  def getMap(id: Int): java.util.TreeMap[String, Int] = {
    if(!entMap.contains(id)){
      new java.util.TreeMap[String, Int]
    } else {
      entMap(id)
    }
  }
  
}

