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

import org.scalatest.FunSuite

class NGramFilter$Test extends FunSuite {

  //Parse tests
  test("containsDictionaryPhrase test 1") {

    val dict = List("ethanol", "hydrogen peroxide").toSet
    assert( NGramFilter.containsDictionaryPhrase( "that ethanol is very flamable", dict) )
    assert( !NGramFilter.containsDictionaryPhrase( "brown dog jumps over blah blah", dict) )
  }

  test("Split string to tokens 1") {
    val splitResultOption = NGramFilter.stringRecordToNgramAndFreq("ethanol is flamable compound\t1995\t10\t5")
    val splitResult = splitResultOption match {
      case Some(x) => x
      case None => ("",0)
    }
    assert( splitResult == ("ethanol is flamable compound", 10) )
  }

}
