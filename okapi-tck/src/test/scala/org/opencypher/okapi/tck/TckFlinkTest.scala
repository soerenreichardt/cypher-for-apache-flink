package org.opencypher.okapi.tck

import java.io.File

import org.opencypher.flink.CAPFGraph
import org.opencypher.flink.test.CAPFTestSuite
import org.opencypher.flink.test.support.capf.{CAPFScanGraphFactory, CAPFTestGraphFactory}
import org.opencypher.okapi.tck.Tags.{BlackList, TckCapfTag, WhiteList}
import org.opencypher.tools.tck.api.CypherTCK
import org.scalatest.prop.TableDrivenPropertyChecks._

import scala.util.{Failure, Success, Try}

class TckFlinkTest extends CAPFTestSuite {

  private val factories = Table(
    "factory",
    CAPFScanGraphFactory
  )

  private val defaultFactory: CAPFTestGraphFactory = CAPFScanGraphFactory

  private val scenarios = ScenariosFor("flink")

  forAll(factories) { factory =>
    forAll(scenarios.whiteList) { scenario =>
      test(s"[${factory.name}, ${WhiteList.name}] $scenario", WhiteList, TckCapfTag) {
        scenario(TCKGraph(factory, CAPFGraph.empty)).execute()
      }
    }
  }

  forAll(scenarios.blackList) { scenario =>
    test(s"[${defaultFactory.name}, ${BlackList.name}] $scenario", BlackList, TckCapfTag) {
      val tckGraph = TCKGraph(defaultFactory, CAPFGraph.empty)

      Try(scenario(tckGraph).execute()) match {
        case Success(_) =>
          throw new RuntimeException(s"A blacklisted scenario actually worked: $scenario")
        case Failure(_) =>
          ()
      }
    }
  }

  ignore("run Custom Scenario") {
    val file = new File(getClass.getResource("CustomTest.feature").toURI)

    CypherTCK
      .parseFilesystemFeature(file)
      .scenarios
      .foreach(scenario => scenario(TCKGraph(defaultFactory, CAPFGraph.empty)).execute())
  }

  it("run Single Scenario") {
    scenarios.get("OPTIONAL MATCH with previously bound nodes")
      .foreach(scenario => scenario(TCKGraph(defaultFactory,  CAPFGraph.empty)).execute())
  }
}
