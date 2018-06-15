package org.opencypher.flink.impl.physical.operators

trait InheritedHeader {
  this: CAPFPhysicalOperator =>
    override val header = children.head.header
}
