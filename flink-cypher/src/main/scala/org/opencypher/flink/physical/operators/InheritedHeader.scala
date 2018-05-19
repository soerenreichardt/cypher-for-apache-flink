package org.opencypher.flink.physical.operators

trait InheritedHeader {
  this: CAPFPhysicalOperator =>
    override val header = children.head.header
}
