/**
Copyright (c) 2015 Alexander Savochkin
Chemical wikipedia search (chwise.net) web-site source code
This file is part of ChWiSe.Net infrastructure.
ChWiSe.Net infrastructure is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.
You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package net.chwise.dataaquisition.textmining

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Set

object NGramFilter {

  def containsDictionaryPhrase(s: String, dict: Set[String]): Boolean = {

    def splitToGrams(maxLength: Int, s: String): Seq[String] = {
      val wordsList = s.split(" ")
      val slided = (for (i <- 1 to maxLength) yield wordsList.sliding(i) )
      val lslided = slided.toList.map(p => p.toList)
      val ngrams = lslided.flatten.map(x => x.mkString(" "))
      ngrams
    }

    val grams = splitToGrams(5, s)

    grams.exists( x => dict.contains(x) )
  }

  def stringRecordToNgramAndFreq(sRecord:String):Option[(String, Int)] = {
    /*
File contains TAB-separated strings:
n-gram - The actual n-gram
year - The year for this aggregation
occurrences - The number of times this n-gram appeared in this year
volume_count - ...
     */
    val splitResult = sRecord.split("\t")
    splitResult match {
      case Array(ngram,year,occurrences,_) => Some( (ngram, occurrences.toInt) )  //2012 version
      case Array(ngram,year,occurrences,_,_) => Some( (ngram, occurrences.toInt) ) //2009 version
      case _ => None
    }
  }



/*
Command line options representing class
*/
  case class CmdlineConfig(
    compoundNamesFilePath:String = "",   //Path to file containing list of all chemical compounds
    ngramsSetPathListFile:String = "",   //Path to (local) file containing list of path (usually in cloud) with n-grams. See PROJECTROOT/data/list-ngrams-s3-pathes.txt
    outputFile:String = "",              //Output path (hdfs or local)
    maxNGramsToProcess:Int = -1,         //By default this programs will process all ngrams, but it is possible to process some smaller ngrams number (for debugging)
    inputInPlainText:Boolean = false,    //google ngrams are stored in S3 in LZO-compressed format. This option enables reading data in plain text (for debugging)
    debug:Boolean = false
  )

  def main(args: Array[String]) {

    val cmdlineParser = new scopt.OptionParser[CmdlineConfig]("scopt") {
        head("Google N-Grams filter for ChWiSe.Net", "0.0.1")
        opt[String]('t', "targetfragments") required() valueName("<file>") action { (f, c) => c.copy(compoundNamesFilePath = f) } text("Path to file containing list of all interesting fragments (chemical compounds for chwise.net)")
        opt[String]('g', "ngramspathfile") required() valueName("<file>") action { (f, c) => c.copy(ngramsSetPathListFile = f) } text("Path to (local) file containing list of path (usually in cloud) with n-grams. See PROJECTROOT/data/list-ngrams-s3-pathes.txt")
        opt[String]('o', "output") required() valueName("<file>") action { (f, c) => c.copy(outputFile = f) } text("Path to (local) file containing list of path (usually in cloud) with n-grams. See PROJECTROOT/data/list-ngrams-s3-pathes.txt")
        opt[Int]('n', "number") action { (n, c) => c.copy(maxNGramsToProcess = n) } text("By default this programs will process all ngrams, but it is possible to process some smaller ngrams number (for debugging)")
        opt[Unit]('p',"plaintextinput") action { (_, c) => c.copy(inputInPlainText = true)  }
        opt[Unit]('d',"debug") action { (_, c) => c.copy(debug = true)  }
    }

    // parser.parse returns Option[C]
    cmdlineParser.parse(args, CmdlineConfig()) match {
        case Some(config) => {
            // do stuff
            val (targetPhrasesFilePath, ngramsSetPathList, outputFile, maxNGramsToProcess, plainTextInput, debug) = config match {
                case CmdlineConfig(p1, p2, p3, n, pt, dbg) => (p1, p2, p3, n, pt, dbg)
            }

            val conf = new SparkConf().setAppName("N-Gram filter")
            val sc = new SparkContext(conf)

            val sourceRDD = if (plainTextInput) {
              val lines = sc.textFile(ngramsSetPathList)
              if (debug)
                lines.saveAsTextFile(outputFile + "_debug_input")
              lines

            }
            else {

              val lines: RDD[String] = sc.sequenceFile(
                ngramsSetPathList,
                classOf[org.apache.hadoop.io.LongWritable],
                classOf[org.apache.hadoop.io.Text], minPartitions = 4) //Min-partitions should depend on the number or cores in your cluster
                .map(_._2.toString)

              //val t = sc.newAPIHadoopFile(ngramsSetPathList, classOf[LzoTextInputFormat], classOf[LongWritable], classOf[Text])
              if (debug)
                lines.saveAsTextFile(outputFile + "_debug_input")
              lines
            }

            val parsedSourceRDDRaw = sourceRDD.map( (p:String) => stringRecordToNgramAndFreq(p) )
            if (debug)
              parsedSourceRDDRaw.saveAsTextFile(outputFile + "_debug_parsedSourceRDDRaw")


            val parsedSourceRDDGoodFormatOption = parsedSourceRDDRaw.filter( (x) => x match { case Some(_) => true; case None => false} )
            if (debug)
              parsedSourceRDDGoodFormatOption.saveAsTextFile(outputFile + "_debug_parsedSourceRDDGoodFormatOption")


            val parsedSourceRDDGoodFormat = parsedSourceRDDGoodFormatOption.map( (x) => x match {case Some(t) => t; case None => ("",0)} ) //None clause must not be called
            if (debug)
              parsedSourceRDDGoodFormat.saveAsTextFile(outputFile + "_debug_parsedSourceRDDGoodFormat")


            val parsedSourceRDDPOSFiltered = parsedSourceRDDGoodFormat.filter(  r => !(r._1 matches ".+_(NOUN|VERB|ADJ|ADV|PRON|DET|ADP|NUM|CONJ|PRT|\\.|X).*" ) ) //Filter pos tagged ngrams
            if (debug)
              parsedSourceRDDPOSFiltered.saveAsTextFile(outputFile + "_debug_parsedSourceRDDPOSFiltered")


            val parsedSourceRDDSentenseMarksFiltered = parsedSourceRDDPOSFiltered.map(r=> ( r._1.replaceAll("_(START|END)_",""), r._2) )
            if (debug)
              parsedSourceRDDSentenseMarksFiltered.saveAsTextFile(outputFile + "_debug_parsedSourceRDDSentenseMarksRemoved")

            val parsedSourceRDDStartEndSpacesRemoved = parsedSourceRDDSentenseMarksFiltered.map(r=>  ( r._1.replaceAll("(^\\s+|\\s+$)",""), r._2) )  //Remove extra spaces
            if (debug)
              parsedSourceRDDStartEndSpacesRemoved.saveAsTextFile(outputFile + "_debug_parsedSourceRDDStartEndSpaceRemoved")

            val parsedSourceRDDSpacesShrinked = parsedSourceRDDStartEndSpacesRemoved.map(r=>  ( r._1.replaceAll("\\s+"," "), r._2) )
            if (debug)
              parsedSourceRDDSpacesShrinked.saveAsTextFile(outputFile + "_debug_parsedSourceRDDSpacesShrinked")

            val parsedSourceRDD = parsedSourceRDDSpacesShrinked.map( r => (r._1.toLowerCase, r._2) )  //lowercase ngrams
            if (debug)
              parsedSourceRDD.saveAsTextFile(outputFile + "_debug_parsedSourceRDDSpacesLowerCased")


            val ngramsWithOccurences = parsedSourceRDD.reduceByKey( _ + _ )

            if (debug)
                ngramsWithOccurences.saveAsTextFile(outputFile + "_debug_ngramsWithOccurences")

            //Read compounds dictionary
            val compoundNamesRDD = sc.textFile(targetPhrasesFilePath)
            val compoundNames:Set[String] = compoundNamesRDD.collect().toSet

            val broadcastCompoundNames = sc.broadcast(compoundNames)

            if (debug) {
              val s:Set[String] = broadcastCompoundNames.value
              val reRDDCompNames = sc.parallelize(s.toSeq)
              reRDDCompNames.saveAsTextFile(outputFile + "_reRDDCompNames")
            }

            //Process n-grams in cluster
            val chemicalNGrams = ngramsWithOccurences.filter( line => containsDictionaryPhrase( line._1, broadcastCompoundNames.value) )
            val chemicalNGramsTSV = chemicalNGrams.map( x=> "%s\t%d".format(x._1, x._2) )

            //Save results
            chemicalNGramsTSV.saveAsTextFile(outputFile)
        }
        case None => {} // arguments are bad, error message will have been displayed
    }    
  }
}
