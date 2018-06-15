package org.opencypher.flink.physical.operators

import org.opencypher.flink.impl.physical.{CAPFPhysicalResult, CAPFRuntimeContext}
import org.opencypher.flink.physical.CAPFRuntimeContext

private[flink] abstract class TernaryPhysicalOperator extends CAPFPhysicalOperator {

  def first: CAPFPhysicalOperator

  def second: CAPFPhysicalOperator

  def third: CAPFPhysicalOperator

  override def execute(implicit context: CAPFRuntimeContext): CAPFPhysicalResult =
    executeTernary(first.execute, second.execute, third.execute)

  def executeTernary(first: CAPFPhysicalResult, second: CAPFPhysicalResult, third: CAPFPhysicalResult)(
    implicit context: CAPFRuntimeContext): CAPFPhysicalResult
}