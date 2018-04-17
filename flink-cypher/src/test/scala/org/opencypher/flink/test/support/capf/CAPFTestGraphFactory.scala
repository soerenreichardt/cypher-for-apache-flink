package org.opencypher.flink.test.support.capf

import org.opencypher.flink.CAPFSession
import org.opencypher.okapi.ir.test.support.creation.TestGraphFactory

trait CAPFTestGraphFactory extends TestGraphFactory[CAPFSession]
