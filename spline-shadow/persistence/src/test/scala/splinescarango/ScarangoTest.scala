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


import java.security.MessageDigest
import java.util.UUID
import java.util.UUID.randomUUID

import com.outr.arango.managed._

import com.outr.arango.{DocumentOption, Edge, _}

import org.apache.commons.lang.builder.ToStringBuilder.reflectionToString
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FunSpec, Matchers}
import za.co.absa.spline.model.{DataLineage, MetaDataSource, MetaDataset}
import za.co.absa.spline.{model => splinemodel}
import za.co.absa.spline.model.dt.Simple
import za.co.absa.spline.model.op.{Generic, BatchWrite, BatchRead, OperationProps}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

case class Progress(timestamp: Long, readCount: Long, _key: Option[String] = None, _id: Option[String] = None,  _rev: Option[String] = None) extends DocumentOption
case class Execution(appId: String, appName: String, sparkVer: String, timestamp: Long, dataTypes: Seq[DataType], _key: Option[String] = None, _id: Option[String] = None, _rev: Option[String] = None) extends DocumentOption
case class DataType(id: String, name: String, nullable: Boolean, childrenIds: Seq[String])
case class Schema(attributes: Seq[Attribute])

trait Operation extends PolymorphicDocumentOption {
  def name: String
  def expression: String
  def outputSchema: Schema
}

case class Read(name: String, expression: String, sourceType: String, outputSchema: Schema, _key: Option[String] = None,   _id: Option[String] = None,  _rev: Option[String] = None, _type: String = "Read") extends Operation
case class Write(name: String, expression: String, destinationType: String, outputSchema: Schema,  _key: Option[String] = None,   _id: Option[String] = None,  _rev: Option[String] = None, _type: String = "Write") extends Operation
case class Process(name: String, expression: String, outputSchema: Schema,  _key: Option[String] = None,   _id: Option[String] = None,  _rev: Option[String] = None, _type: String = "Process") extends Operation

case class DataSource(uri: String, _key: Option[String] = None, _rev: Option[String] = None, _id: Option[String] = None) extends DocumentOption
case class Attribute(name: String, dataTypeId: String)

case class ProgressOf(_from: String, _to: String,_key: Option[String] = None,  _id: Option[String] = None, _rev: Option[String] = None) extends Edge with DocumentOption
case class Follows(_from: String, _to: String,_key: Option[String] = None,  _id: Option[String] = None, _rev: Option[String] = None) extends Edge with DocumentOption
case class ReadsFrom( _from: String, _to: String, _key: Option[String] = None,  _id: Option[String] = None, _rev: Option[String] = None) extends Edge with DocumentOption
case class WritesTo(_from: String, _to: String,  _key: Option[String] = None, _id: Option[String] = None,  _rev: Option[String] = None) extends Edge with DocumentOption
case class Implements(_from: String, _to: String, _key: Option[String] = None,  _id: Option[String] = None,  _rev: Option[String] = None) extends Edge with DocumentOption

object Database extends Graph("lineages") {

  val progress: VertexCollection[Progress] = vertex[Progress]("progress")
  val execution: VertexCollection[Execution] = vertex[Execution]("execution")

  val operation: PolymorphicVertexCollection[Operation] = polymorphic3[Operation, Read, Write, Process]("operation")
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

  // TODO replace with upsert
  private def getHash(s: String) = {
    MessageDigest.getInstance("SHA-256")
      .digest(s.getBytes("UTF-8"))
      .map("%02x".format(_)).mkString
  }

  describe("scarango") {
    it("funspec") {
      awaitForever(Database.delete(true))
      val result = awaitForever(Database.init(force = true))
      save(datalineage())
    }

  }

  private def findOutputSchema(dataLineage: DataLineage, operation: splinemodel.op.Operation): Schema = {
    val metaDataset: MetaDataset = dataLineage.datasets.find((dts: MetaDataset) => dts.id == operation.mainProps.output).get
    val attributes = metaDataset.schema.attrs.map(attrId => {
      val attribute = dataLineage.attributes.find(_.id == attrId).get
      Attribute(attribute.name, attribute.dataTypeId.toString)
    })
    Schema(attributes)
  }

  private def awaitForever(future: Future[_]) = {
    Await.result(future, Duration.Inf)
  }

  def save(dataLineage: DataLineage): Unit = {

    val operations: Seq[Operation] = dataLineage.operations.map(op => {
      val outputSchema = findOutputSchema(dataLineage, op)
      val _key = Some(op.mainProps.id.toString)
      val expression = reflectionToString(op)
      val name = op.mainProps.name
      op match {
        case r: splinemodel.op.Read => Read(name, expression, r.sourceType, outputSchema, _key)
        case w: splinemodel.op.Write => Write(name, expression, w.destinationType, outputSchema, _key)
        case _ => Process(name, expression, outputSchema, None, _key)
    }})
    operations.foreach(op => awaitForever(Database.operation.upsert(op)))


    val outputToOperationId = dataLineage
      .operations
      .map(o => (o.mainProps.output, o.mainProps.id))
      .toMap

    /*
      Operation inputs and outputs ids may be shared across already linked lineages. To avoid saving linked lineages or
      duplicate indexes we need to not use these.
     */
    dataLineage.operations.foreach(op => {
      op.mainProps.inputs
        .flatMap(outputToOperationId.get)
        .map(opId => Follows("operation/" + op.mainProps.id.toString, "operation/" + opId.toString))
        .foreach(f => awaitForever(Database.follows.insert(f)))
    })

    val dataSources = dataLineage.operations.flatMap(op => op match {
      case r: splinemodel.op.Read => r.sources.map(s => s.path)
      case w: splinemodel.op.Write => Some(w.path)
      case _ => None})
      .distinct
      .map(path => DataSource(path, Some(getHash(path))))
    dataSources.foreach(d => awaitForever(Database.dataSource.upsert(d)))

    val writesTos: Seq[WritesTo] = dataLineage.operations.filter(_.isInstanceOf[splinemodel.op.Write]).map(_.asInstanceOf[splinemodel.op.Write])
      .map(o => WritesTo("operation/" + o.mainProps.id.toString, "dataSource/" + getHash(o.path), Some(o.mainProps.id.toString)))
    writesTos.foreach(w => awaitForever(Database.writesTo.upsert(w)))

    val readsFroms = dataLineage.operations
      .filter(_.isInstanceOf[splinemodel.op.Read])
      .map(_.asInstanceOf[splinemodel.op.Read])
      .flatMap(op => op.sources.map(s => ReadsFrom("operation/" + op.mainProps.id.toString, "dataSource/" + getHash(s.path), Some(op.mainProps.id.toString))))
    readsFroms.foreach(r => awaitForever(Database.readsFrom.upsert(r)))

    val dataTypes = dataLineage.dataTypes.map(d => DataType(d.id.toString, d.getClass.getSimpleName, d.nullable, d.childDataTypeIds.map(_.toString)))
    val execution = Execution(dataLineage.appId, dataLineage.appName, dataLineage.sparkVer, dataLineage.timestamp, dataTypes, Some(dataLineage.id.toString))
    awaitForever(Database.execution.upsert(execution))

    //  progress for batch need to be generated during migration
    val progress = dataLineage.operations.find(_.isInstanceOf[BatchWrite]).map(_ => Progress(dataLineage.timestamp, -1, Some(dataLineage.id.toString)))
    progress.foreach(p => awaitForever(Database.progress.insert(p)))

    val progressOf = progress.map(p => ProgressOf("progress/" + p._key.get, "execution/" +  execution._key.get, p._key))
    progressOf.foreach(p => awaitForever(Database.progressOf.insert(p)))

    val implements = Implements("execution/" + execution._key.get, "operation/" + dataLineage.rootOperation.mainProps.id.toString, execution._key)
    awaitForever(Database.implements.insert(implements))

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
      val mdInput = MetaDataset(randomUUID, bSchema)

      DataLineage(
        appId,
        appName,
        timestamp,
        "2.3.0",
        Seq(
          BatchWrite(OperationProps(randomUUID, "Write", Seq(md3.id), mdOutput.id), "parquet", path, append, null, null),
          Generic(OperationProps(randomUUID, "Filter", Seq(md4.id), md2.id), "rawString2"),
          BatchRead(OperationProps(randomUUID, "BatchRead", Seq(mdInput.id), md4.id), "csv", Seq(MetaDataSource("hdfs://catSizes/brownCats", Seq(randomUUID)))),
          Generic(OperationProps(randomUUID, "Filter", Seq(md4.id), md1.id), "rawString4"),
          Generic(OperationProps(randomUUID, "Union", Seq(md1.id, md2.id), md3.id), "rawString1")
        ),
        Seq(mdOutput, md1, md2, md3, md4),
        attributes,
        dataTypes
      )
    }

}

