package org.opencypher.morpheus

import java.net.URI

import org.opencypher.morpheus.api.{GraphSources, MorpheusSession}
import org.opencypher.morpheus.impl.MorpheusRecords
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.PrintRelationalPlan

object OrcDemo extends App {

  implicit val session: MorpheusSession = MorpheusSession.local()

  //  val fs = FileSystem.get(new URI("hdfs://foo"))

  val orcFolder = new URI("/home/soeren/Dev/s3/orc").getPath
  session.registerSource(Namespace("orc"), GraphSources.fs(orcFolder).orc)

  PrintRelationalPlan.set

  val t = session.cypher(
    //    """
    //      |FROM GRAPH orc.sf1
    //      |MATCH (p:Post)-[:POST_HAS_CREATOR]->(t:Person)
    //      |RETURN p, t
    //      |ORDER BY p
    //      |LIMIT 20
    //    """.stripMargin
    """
      |FROM GRAPH orc.sf1
      |MATCH (m:Message) RETURN m LIMIT 100
    """.stripMargin
  ).records.asInstanceOf[MorpheusRecords]
  t.show

}
