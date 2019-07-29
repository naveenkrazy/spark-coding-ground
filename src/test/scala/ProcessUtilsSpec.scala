import com.secureworks.codingchallenge.ProcessUtils
import org.scalatest.{FunSpec, GivenWhenThen}

class ProcessUtilsSpec extends FunSpec with GivenWhenThen with SparkTest {

  describe("Testing Extract parameters method..") {
    it("should extract parameters as Map given the list of environment args array") {
      Given("Command line arguments array")
      val simpleArgs = Array("--class", "TestClass", "--topRecordsSize", "5", "--consoleResultSize", "20")
      val invalidArgs = Array("class", "TestClass", "--topRecordsSize", "5", "--consoleResultSize", "20")

      And("expected results maps defined...")
      val resultMap1 = Map("class" -> "TestClass", "topRecordsSize" -> "5", "consoleResultSize" -> "20")

      val resultMap2 = Map("topRecordsSize" -> "5", "consoleResultSize" -> "20")

      Then("test against the expected Result")
      assert(resultMap1 sameElements ProcessUtils.extractParameters(simpleArgs))
      assert(resultMap2 sameElements ProcessUtils.extractParameters(invalidArgs))
    }

  }

  describe("Test for extracting patterns from input string") {
    it("should extract pattern from given regex map") {
      Given("map with pattern names and regex patterns")
      val patternMap = Map(
        "host" -> """(\S+\.[\S+\.]+\S+)""".r,
        "request" -> """\"(\S+)\s(\S+)\s*(\S*)\"""".r,
        "httpResponse" -> """\s(\d{3})\s""".r,
        "response" -> """\s(\d+)$""".r
      )

      And("Source String to extract matching data")
      val source1 = """ppp-mia-30.shadow.net - - [01/Jul/1995:00:00:43 -0400] "GET /images/WORLD-logosmall.gif HTTP/1.0" 200 669"""
      val source2 ="""199.72.81.55 - - [01/Jul/1995:00:00:59 -0400] "GET /history/ HTTP/1.0" 200 1382"""

      And("expected results after extracting patterns....")
      val res1 = Map("host" -> "ppp-mia-30.shadow.net",
        "request" -> "GET /images/WORLD-logosmall.gif HTTP/1.0",
        "httpResponse" -> "200",
        "response" -> ""
      )

      Then("test the result data against expected string")

      ProcessUtils.extractPattern(patternMap, source1)


    }
  }






}
