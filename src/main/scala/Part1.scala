import org.apache.spark.{SparkConf, SparkContext}

object Part1 {
  def main(args: Array[String]): Unit = {

    // get the spark contextwith config settings from commandline
    val sc = new SparkContext(new SparkConf())
    // get the inputdirectory from command line
    val input = sc.textFile(args(0))
    val reg = "((?:[^\",]|(?:\"(?:\\{2}|\"|[^\"])*?\"))*)".r;
    //(reg findAllIn(line)).toList.filter(item => (item.toString != ""))
    // word count logic
    val count = input.map{line => buildMoveEntry((reg findAllIn(line)).toList.map(_.toString))}
      .reduceByKey(_+_)
      .filter(_._2.rateCount > 0)
      .map(v => (v._2.rateCount, v._2.rateCount.toString + "   " + v._2.mTitle))
      .sortByKey()
      .values

    // specify the output directory from command line
    count.saveAsTextFile(args(1))
  }

  def buildMoveEntry(info: List[String]): (String,MovieEntry) = {
    var result: MovieEntry = null;
    var key:String = "";
    if (info.size == 8) {//userId,movieId,rating,timestamp
      key = info(2);
      result = new MovieEntry(key,"", 1);
    }
    if (info.size == 6) { //movieId,title,genres
      key = info(0);
      result = new MovieEntry(key, info(2), 0);
    }
    return key->result;
  }
}
