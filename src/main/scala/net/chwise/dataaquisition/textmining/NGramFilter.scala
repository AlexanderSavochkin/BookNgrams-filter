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

import scala.io.Source
import scala.collection.immutable.Set

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import com.hadoop.mapreduce.LzoTextInputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text

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

  def stringRecordToNgramAndFreq(sRecord:String):(String, Int) = {
    /*
File contains TAB-separated strings:
n-gram - The actual n-gram
year - The year for this aggregation
occurrences - The number of times this n-gram appeared in this year
pages - The number of pages this n-gram appeared on in this year
books - The number of books this n-gram appeared in during this year
     */
    sRecord.split("\t") match {
      case Array(ngram,year,occurrences,pages,books) => (ngram, occurrences.toInt)
    }
  }


/*
Command line options representing class
*/
  case class CmdlineConfig(
    compoundNamesFilePath:String = "",   //Path to file containing list of all chemical compounds
    ngramsSetPathListFile:String = "",   //Path to (local) file containing list of path (usually in cloud) with n-grams. See PROJECTROOT/data/list-ngrams-s3-pathes.txt
    outputFile:String = "",              //Output path (hdfs or local)
    maxNGramsToProcess:Int = -1          //By default this programs will process all ngrams, but it is possible to process some smaller ngrams number (for debugging)
  )

  def main(args: Array[String]) {

    val cmdlineParser = new scopt.OptionParser[CmdlineConfig]("scopt") {
        head("Google N-Grams filter for ChWiSe.Net", "0.0.1")
        opt[String]('t', "targetfragments") required() valueName("<file>") action { (f, c) => c.copy(compoundNamesFilePath = f) } text("Path to file containing list of all interesting fragments (chemical compounds for chwise.net)")
        opt[String]('g', "ngramspathfile") required() valueName("<file>") action { (f, c) => c.copy(ngramsSetPathListFile = f) } text("Path to (local) file containing list of path (usually in cloud) with n-grams. See PROJECTROOT/data/list-ngrams-s3-pathes.txt")
        opt[String]('o', "output") required() valueName("<file>") action { (f, c) => c.copy(outputFile = f) } text("Path to (local) file containing list of path (usually in cloud) with n-grams. See PROJECTROOT/data/list-ngrams-s3-pathes.txt")
        opt[Int]('n', "number") action { (n, c) => c.copy(maxNGramsToProcess = n) } text("By default this programs will process all ngrams, but it is possible to process some smaller ngrams number (for debugging)")
    }

    // parser.parse returns Option[C]
    cmdlineParser.parse(args, CmdlineConfig()) match {
        case Some(config) => {
            // do stuff
            val (targetPhrasesFilePath, ngramsSetPathList, outputFile, maxNGramsToProcess) = config match {
                case CmdlineConfig(p1, p2, p3, n) => (p1, p2, p3, n)
            }

            val conf = new SparkConf().setAppName("N-Gram filter")
            val sc = new SparkContext(conf)

            val sourceRDD = sc.newAPIHadoopFile(ngramsSetPathList, classOf[LzoTextInputFormat], classOf[LongWritable], classOf[Text])
            val ngramsWithOccurences = sourceRDD.map( (p:(LongWritable,Text)) => stringRecordToNgramAndFreq(p._2.toString()) ).reduceByKey( _ + _ )

            //Read compounds dictionary
            val compoundNamesRDD = sc.textFile(targetPhrasesFilePath)
            val compoundNames:Set[String] = compoundNamesRDD.collect().toSet
            val broadcastCompoundNames = sc.broadcast(compoundNames)

            //Process n-grams in cluster
            val chemicalNGrams = ngramsWithOccurences.filter( line => containsDictionaryPhrase( line._1, broadcastCompoundNames.value) )
            val chemicalNGramsTSV = chemicalNGrams.map( x=> "%s\t%d".format(x._1, x._2) )

            //Save results
            chemicalNGramsTSV.saveAsTextFile(outputFile)

            println("Done")
        }
        case None => {} // arguments are bad, error message will have been displayed
    }    
  }
}
