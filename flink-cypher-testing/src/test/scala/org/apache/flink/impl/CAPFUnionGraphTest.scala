package org.apache.flink.impl

import org.opencypher.flink.test.CAPFTestSuite
import org.opencypher.flink.test.fixture.{GraphConstructionFixture, RecordsVerificationFixture, TeamDataFixture}

class CAPFUnionGraphTest extends CAPFTestSuite
  with GraphConstructionFixture
  with RecordsVerificationFixture
  with TeamDataFixture {

  import CAPFGraphTestData._

  def testGraph1 = initGraph("CREATE (:Person {name: 'Mats'})")
  def testGraph2 = initGraph("CREATE (:Person {name: 'Phil'})")

  it("supports UNION ALL") {
    testGraph1.unionAll(testGraph2).cypher("""MATCH (n) RETURN DISTINCT id(n)""").getRecords.size should equal(2)
  }
}
