package splinescarango

/*
 * Copyright 2017 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Copyright 2017 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.util.UUID
import java.util.UUID.randomUUID

import com.outr.arango.{DocumentOption, Edge}
import com.outr.arango.managed._
import org.apache.commons.lang.builder.ToStringBuilder.reflectionToString
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FunSpec, Matchers}
import za.co.absa.spline.model.{DataLineage, MetaDataset}
import za.co.absa.spline.{model => splinemodel}
import za.co.absa.spline.model.dt.Simple
import za.co.absa.spline.model.op.{Generic, Read, Write, BatchWrite, OperationProps}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

case class Progress(timestamp: Long, readCount: Long, _key: Option[String] = None, _id: Option[String] = None,  _rev: Option[String] = None) extends DocumentOption
case class Execution(appId: String, appName: String, sparkVer: String, timestamp: Long, dataTypes: Seq[DataType], _key: Option[String] = None, _id: Option[String] = None, _rev: Option[String] = None) extends DocumentOption
case class DataType(id: String, name: String, nullable: Boolean, childrenIds: Seq[String])
case class Schema(attributes: Seq[Attribute])//, dataTypes: Seq[DataType])
case class Operation(name: String, expression: String, outputSchema: Schema, _key: Option[String] = None,  _id: Option[String] = None, _rev: Option[String] = None) extends DocumentOption
case class DataSource(`type`: String, path: String, _key: Option[String] = None, _rev: Option[String] = None, _id: Option[String] = None) extends DocumentOption
case class Attribute(name: String, dataTypeId: String)

case class ProgressOf(_from: String, _to: String,_key: Option[String] = None,  _id: Option[String] = None, _rev: Option[String] = None) extends Edge with DocumentOption
case class Follows(_from: String, _to: String,_key: Option[String] = None,  _id: Option[String] = None, _rev: Option[String] = None) extends Edge with DocumentOption
case class ReadsFrom( _from: String, _to: String, _key: Option[String] = None,  _id: Option[String] = None, _rev: Option[String] = None) extends Edge with DocumentOption
case class WritesTo(_from: String, _to: String,  _key: Option[String] = None, _id: Option[String] = None,  _rev: Option[String] = None) extends Edge with DocumentOption
case class Implements(_from: String, _to: String, _key: Option[String] = None,  _id: Option[String] = None,  _rev: Option[String] = None) extends Edge with DocumentOption

object Database extends Graph("lineages") {
  val progress: VertexCollection[Progress] = vertex[Progress]("progress")
  val execution: VertexCollection[Execution] = vertex[Execution]("execution")
  val operation: VertexCollection[Operation] = vertex[Operation]("operation")
  val dataSource: VertexCollection[DataSource] = vertex[DataSource]("dataSource")

  val progressOf: EdgeCollection[ProgressOf] = edge[ProgressOf]("progressOf", ("progress", "execution"))
  val follows: EdgeCollection[Follows] = edge[Follows]("follows", ("operation", "operation"))
  val readsFrom: EdgeCollection[ReadsFrom] = edge[ReadsFrom]("readsFrom", ("operation", "dataSource"))
  val writesTo: EdgeCollection[WritesTo] = edge[WritesTo]("writesTo", ("operation", "dataSource"))
  val implements: EdgeCollection[Implements] = edge[Implements]("implements", ("execution", "operation"))

  trait CopyMethod[T <: DocumentOption] extends DocumentOption {
    def copy(_key: Option[String], _id: Option[String], _rev: Option[String]): T
  }

}

class ScarangoTest extends FunSpec with Matchers with MockitoSugar {

  describe("scarango") {
    it("funspec") {
      val result = awaitForever(Database.init(force = true))
      save(datalineage())
    }

  }

  // TODO store full attribute information
  private def findOutputSchema(dataLineage: DataLineage, operation: splinemodel.op.Operation): Schema = {
    val metaDataset: MetaDataset = dataLineage.datasets.find((dts: MetaDataset) => dts.id == operation.mainProps.output).get
    val attributes = metaDataset.schema.attrs.map(attrId => {
      val attribute = dataLineage.attributes.find(_.id == attrId).get
//      (attribute.name, dataLineage.dataTypes.find(attribute.dataTypeId == _.id).get.getClass.getSimpleName)
      Attribute(attribute.name, attribute.dataTypeId.toString)
    })
    Schema(attributes) //, dataLineage.dataTypes)
  }

  private def awaitForever(future: Future[_]) = {
    Await.result(future, Duration.Inf)
  }

  def save(dataLineage: DataLineage): Unit = {

    val operations: Seq[Operation] = dataLineage.operations.map(op => {
      val outputSchema = findOutputSchema(dataLineage, op)
      Operation(op.mainProps.name, reflectionToString(op), outputSchema, Some(op.mainProps.output.toString))
    })
    operations.foreach(op => awaitForever(Database.operation.upsert(op)))

    dataLineage.operations.foreach(op => {
      op.mainProps.inputs
        .map(mdid => Follows("operation/" + op.mainProps.output.toString, "operation/" + mdid.toString))
        .foreach(f => awaitForever(Database.follows.insert(f)))
    })

    val dataSources = dataLineage.operations.flatMap(op => op match {
      case r: Read => r.sources.map(s => DataSource(r.sourceType, s.path, Some(r.mainProps.id.toString)))
      case w: Write => Some(DataSource(w.destinationType, w.path, Some(w.mainProps.id.toString)))
      case _ => None
    })
    dataSources.foreach(d => awaitForever(Database.dataSource.upsert(d)))

    val writesTos: Seq[WritesTo] = dataLineage.operations.filter(_.isInstanceOf[Write]).map(o => WritesTo("operation/" + o.mainProps.id.toString, "dataSource/" + o.mainProps.id.toString, Some(o.mainProps.id.toString)))
    writesTos.foreach(w => awaitForever(Database.writesTo.upsert(w)))

    val readsFroms = dataLineage.operations
      .filter(_.isInstanceOf[Read])
      .map(_.asInstanceOf[Read])
      .flatMap(op => op.sources.map(s => ReadsFrom("operation/" + op.mainProps.id.toString, "dataSource/" + op.mainProps.id.toString, Some(op.mainProps.id.toString))))
    readsFroms.foreach(r => awaitForever(Database.readsFrom.upsert(r)))

    val dataTypes = dataLineage.dataTypes.map(d => DataType(d.id.toString, d.getClass.getSimpleName, d.nullable, d.childDataTypeIds.map(_.toString)))
    val execution = Execution(dataLineage.appId, dataLineage.appName, dataLineage.sparkVer, dataLineage.timestamp, dataTypes, Some(dataLineage.id.toString))
    awaitForever(Database.execution.upsert(execution))
//  progress for batch need to be generated during migration
    val progress = dataLineage.operations.find(_.isInstanceOf[BatchWrite]).map(_ => Progress(dataLineage.timestamp, -1, Some(dataLineage.id.toString)))
    progress.foreach(p => awaitForever(Database.progress.insert(p)))

    val progressOf = progress.map(p => ProgressOf("progress/" + p._key.get, "execution/" +  execution._key.get, p._key))
    progressOf.foreach(p => awaitForever(Database.progressOf.insert(p)))

    // TODO implements
    val implements = Implements("execution/" + execution._key.get, "operation/" + dataLineage.rootOperation.mainProps.output.toString, execution._key)
    awaitForever(Database.implements.insert(implements))

    // TODO writesTo, readsFrom:
    // TODO Lineages are connected via meta dataset ids, should we store that somehow in progress events as well?
    null
  }

  private def datalineage(
                           appId: String = "appId1",
                           appName: String = "appName1",
                           timestamp: Long = 123L,
                           datasetId: UUID = randomUUID,
                           path: String = "hdfs://foo/bar/path",
                           append: Boolean = false)
    : DataLineage = {
      val dataTypes = Seq(Simple("StringType", nullable = true))

      val attributes = Seq(
        splinemodel.Attribute(randomUUID(), "_1", dataTypes.head.id),
        splinemodel.Attribute(randomUUID(), "_2", dataTypes.head.id),
        splinemodel.Attribute(randomUUID(), "_3", dataTypes.head.id)
      )
      val aSchema = splinemodel.Schema(attributes.map(_.id))
      val bSchema = splinemodel.Schema(attributes.map(_.id).tail)

      val md1 = MetaDataset(datasetId, aSchema)
      val md2 = MetaDataset(randomUUID, aSchema)
      val md3 = MetaDataset(randomUUID, bSchema)
      val md4 = MetaDataset(randomUUID, bSchema)
      val mdOutput = MetaDataset(randomUUID, bSchema)

      DataLineage(
        appId,
        appName,
        timestamp,
        "2.3.0",
        Seq(
          BatchWrite(OperationProps(randomUUID, "Write", Seq(md3.id), mdOutput.id), "parquet", path, append),
          Generic(OperationProps(randomUUID, "Filter", Seq(md4.id), md2.id), "rawString2"),
          Generic(OperationProps(randomUUID, "LogicalRDD", Seq.empty, md4.id), "rawString3"),
          Generic(OperationProps(randomUUID, "Filter", Seq(md4.id), md1.id), "rawString4"),
          Generic(OperationProps(randomUUID, "Union", Seq(md1.id, md2.id), md3.id), "rawString1")
        ),
        Seq(mdOutput, md1, md2, md3, md4),
        attributes,
        dataTypes
      )
    }

}

