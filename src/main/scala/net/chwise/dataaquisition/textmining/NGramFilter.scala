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

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object NGramFilter {

  val googleNGramsS3URL = "s3n://datasets.elasticmapreduce/ngrams/books/"

  def readCompoundsDictionary(path:String):Set[String] = {
    Source.fromFile(path).getLines.map( s => s.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]+", " ") ).toSet
  }

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

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("N-Gram filter")
    val sc = new SparkContext(conf)

    val compoundNamesPath = args(0);

    val dataSetURL = if (args.length > 1) args(1) else googleNGramsS3URL;

    val ngramsDatasetRows = sc.textFile(dataSetURL, 2).cache()

    val ngramsWithOccurences = ngramsDatasetRows.map( t => stringRecordToNgramAndFreq(t) ).reduceByKey( _ + _ )

    //Read compounds dictionary
    val compoundNames = readCompoundsDictionary(compoundNamesPath)
    val broadcastCompoundNames = sc.broadcast(compoundNames)

    //Process n-grams in cluster
    val chemicalNGrams = ngramsWithOccurences.filter( line => containsDictionaryPhrase( line._1, broadcastCompoundNames.value) )
    val chemicalNGramsTSV = chemicalNGrams.map( x=> "%s\t%d".format(x._1, x._2) )

    //Save results
    //chemicalNGramsTSV.saveAsTextFile("hdfs://chwise/chemical-n-grams.txt")
    chemicalNGramsTSV.saveAsTextFile("/home/asavochkin/Work/Projects/ChWiSe/Data/result.txt")

    println("Done")
  }
}
