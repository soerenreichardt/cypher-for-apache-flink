package org.opencypher.flink

import org.opencypher.okapi.api.schema.{LabelPropertyMap, RelTypePropertyMap, Schema}
import org.opencypher.okapi.impl.exception.{SchemaException, UnsupportedOperationException}
import org.opencypher.okapi.impl.schema.SchemaUtils._
import org.opencypher.flink.CAPFCypherType._
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.types.{CTRelationship, CypherType}
import org.opencypher.okapi.impl.schema.{ImpliedLabels, LabelCombinations}

object CAPFSchema {
  val empty: CAPFSchema = Schema.empty.asCapf

  implicit class CAPFSchemaConverter(schema: Schema) {

    def asCapf: CAPFSchema = {
      schema match {
        case s: CAPFSchema => s
        case s: Schema =>
          val combosByLabel = s.foldAndProduce(Map.empty[String, Set[Set[String]]])(
            (set, combos, _) => set + combos,
            (combos, _) => Set(combos))

          combosByLabel.foreach {
            case (_, combos) =>
              val keysForAllCombosOfLabel = combos.map(combo => combo -> schema.nodeKeys(combo))
              for {
                (combo1, keys1) <- keysForAllCombosOfLabel
                (combo2, keys2) <- keysForAllCombosOfLabel
              } yield {
                (keys1.keySet intersect keys2.keySet).foreach { k =>
                  val t1 = keys1(k)
                  val t2 = keys2(k)
                  val join = t1.join(t2)
                  if (!join.isFlinkCompatible) {
                    val explanation = if (combo1 == combo2) {
                      s"The unsupported type is specified on label combination ${combo1.mkString("[", ", ", "]")}."
                    } else {
                      s"The conflict appears between label combinations ${combo1.mkString("[", ", ", "]")} and ${combo2.mkString("[", ", ", "]")}."
                    }
                    throw SchemaException(s"The property type '$join' for property '$k' can not be stored in a Spark column. " + explanation)
                  }
                }
              }
          }

          new CAPFSchema(s)

        case other => throw UnsupportedOperationException(s"${other.getClass.getSimpleName} does not have Tag support")

      }
    }
  }
}

case class CAPFSchema private(schema: Schema) extends Schema {

  override def labels: Set[String] = schema.labels

  override def relationshipTypes: Set[String] = schema.relationshipTypes

  override def labelPropertyMap: LabelPropertyMap = schema.labelPropertyMap

  override def relTypePropertyMap: RelTypePropertyMap = schema.relTypePropertyMap

  override def impliedLabels: ImpliedLabels = schema.impliedLabels

  override def labelCombinations: LabelCombinations = schema.labelCombinations

  override def impliedLabels(knownLabels: Set[String]): Set[String] = schema.impliedLabels(knownLabels)

  override def nodeKeys(labels: Set[String]): PropertyKeys = schema.nodeKeys(labels)

  override def allNodeKeys: PropertyKeys = schema.allNodeKeys

  override def allLabelCombinations: Set[Set[String]] = schema.allLabelCombinations

  override def combinationsFor(knownLabels: Set[String]): Set[Set[String]] = schema.combinationsFor(knownLabels)

  override def nodeKeyType(labels: Set[String], key: String): Option[CypherType] = schema.nodeKeyType(labels, key)

  override def keysFor(labelCombinations: Set[Set[String]]): PropertyKeys = schema.keysFor(labelCombinations)

  override def relationshipKeyType(types: Set[String], key: String): Option[CypherType] = schema.relationshipKeyType(types, key)

  override def relationshipKeys(typ: String): PropertyKeys = schema.relationshipKeys(typ)

  override def withNodePropertyKeys(nodeLabels: Set[String], keys: PropertyKeys): Schema = schema.withNodePropertyKeys(nodeLabels, keys)

  override def withRelationshipPropertyKeys(typ: String, keys: PropertyKeys): Schema = schema.withRelationshipPropertyKeys(typ, keys)

  override def ++(other: Schema): Schema = schema ++ other

  override def pretty: String = schema.pretty

  override def isEmpty: Boolean = schema.isEmpty

  override def forNode(labelConstraints: Set[String]): Schema = schema.forNode(labelConstraints)

  override def forRelationship(relType: CTRelationship): Schema = schema.forRelationship(relType)

  override def dropPropertiesFor(combo: Set[String]): Schema = schema.dropPropertiesFor(combo)

  override def withOverwrittenNodePropertyKeys(nodeLabels: Set[String], propertyKeys: PropertyKeys): Schema = schema.withOverwrittenNodePropertyKeys(nodeLabels, propertyKeys)

  override def withOverwrittenRelationshipPropertyKeys(relType: String, propertyKeys: PropertyKeys): Schema = schema.withOverwrittenRelationshipPropertyKeys(relType, propertyKeys)
}
