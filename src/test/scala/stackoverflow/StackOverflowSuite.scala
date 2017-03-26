package stackoverflow

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120

    lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
    lazy val sc: SparkContext = new SparkContext(conf)

    def lines: RDD[String] = sc.textFile("../../src/main/resources/stackoverflow/stackoverflow.csv").cache()
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("Loading data file succeeds") {
    assert(testObject.lines.count() == 8143801, "Input file doesn't load correctly.")
  }

  test("Parsing Postings works fine") {
    val linesCount = testObject.lines.count()
    val raw = testObject.rawPostings(testObject.lines).count()

    assert(linesCount == raw, s"Parsing data has failed (lines $linesCount, but only $raw postings parsed")
  }

  test("Grouping Postings looks fine") {
    val raw = testObject.rawPostings(testObject.lines)
    val grouped = testObject.groupedPostings(raw)

    assert(grouped.count() == 4160520, "Grouping doesn't do a thing right")
  }

  test("Scored RDD should contain the following tuples") {
    val raw = testObject.rawPostings(testObject.lines)
    val grouped = testObject.groupedPostings(raw)
    val scored = testObject.scoredPostings(grouped).cache()

    def containsRankingById(postingId:Int, ranking: Int): Boolean = {
      scored.filter( { case (posting, rank) => ranking == rank && posting.id == postingId }).count() > 0
    }

    assert(containsRankingById(6, 67), "should contain the following tuple ((1,6,None,None,140,Some(CSS)),67)")
    assert(containsRankingById(42, 89), "should contain the following tuple ((1,42,None,None,155,Some(PHP)),89)")
    assert(containsRankingById(72, 3), "should contain the following tuple ((1,72,None,None,16,Some(Ruby)),3)")
    assert(containsRankingById(126, 30), "should contain the following tuple ((1,126,None,None,33,Some(Java)),30)")
    assert(containsRankingById(174, 20), "should contain the following tuple ((1,174,None,None,38,Some(C#)),20)")
  }

//  test("Loading raw postings work") {
//    val raw = testObject.rawPostings(testObject.lines)
//    val grouped = testObject.groupedPostings(raw)
//    val scored = testObject.scoredPostings(grouped)
//
//    val vectors = testObject.vectorPostings(scored)
    //    val means = kmeans(sampleVectors(vectors), vectors, debug = true)
    //    val results = clusterResults(means, vectors)
//  }
}
