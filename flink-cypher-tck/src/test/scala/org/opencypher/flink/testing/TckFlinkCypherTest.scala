package org.opencypher.flink.testing

import java.io.File

import org.opencypher.flink.impl.CAPFGraph
import org.opencypher.flink.test.CAPFTestSuite
import org.opencypher.flink.test.support.capf.{CAPFScanGraphFactory, CAPFTestGraphFactory}
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.PrintPhysicalPlan
import org.opencypher.okapi.tck.test.Tags.{BlackList, WhiteList}
import org.opencypher.okapi.tck.test.{ScenariosFor, TCKGraph}
import org.opencypher.tools.tck.api.CypherTCK
import org.scalatest.Tag
import org.scalatest.prop.TableDrivenPropertyChecks._

import scala.util.{Failure, Success, Try}

class TckFlinkCypherTest extends CAPFTestSuite {

  object TckCapfTag extends Tag("TckFlinkCypher")

  private val factories = Table(
    ("factory", "additional_blacklist"),
    (CAPFScanGraphFactory, Set.empty[String])
  )

  private val defaultFactory: CAPFTestGraphFactory = CAPFScanGraphFactory

  private val blacklistFile = getClass.getResource("/scenario_blacklist").getFile
  private val scenarios = ScenariosFor(blacklistFile)

  PrintPhysicalPlan.set()

  forAll(factories) { (factory, additional_blacklist) =>
    forAll(scenarios.whiteList) { scenario =>
      if (!additional_blacklist.contains(scenario.toString)) {
        test(s"[${factory.name}, ${WhiteList.name}] $scenario", WhiteList, TckCapfTag, Tag(factory.name)) {
          scenario(TCKGraph(factory, CAPFGraph.empty)).execute()
        }
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
      .foreach(scenario => scenario(TCKGraph(defaultFactory,  CAPFGraph.empty)).execute())
  }

  it("run Single Scenario") {
    scenarios.get("Multiple WITHs using a predicate and aggregation")
      .foreach(scenario => scenario(TCKGraph(defaultFactory, CAPFGraph.empty)).execute())
  }

}
