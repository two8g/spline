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

import io.circe.ObjectEncoder

//import ArangoModel._
import com.outr.arango._
import com.outr.arango.managed._
import org.apache.commons.lang.builder.ToStringBuilder.reflectionToString
import org.scalatest.mockito.MockitoSugar
import org.scalatest.tools.Durations
import org.scalatest.{FunSpec, Matchers}
import za.co.absa.spline.model.{DataLineage, MetaDataset}
import za.co.absa.spline.{model => splinemodel}
import za.co.absa.spline.model.dt.{DataType, Simple}
import za.co.absa.spline.model.op.{BatchWrite, Generic, OperationProps}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

//object ArangoModel {
  case class Progress(timestamp: Long, readCount: Long, _key: Option[String] = None, _id: Option[String] = None,  _rev: Option[String] = None) extends DocumentOption
  case class Execution(appId: String, appName: String, sparkVer: String, timestamp: Long, _key: Option[String] = None, _id: Option[String] = None, _rev: Option[String] = None) extends DocumentOption

  case class Schema(attributes: Seq[Attribute])//, dataTypes: Seq[DataType])
  case class Operation(name: String, expression: String, outputSchema: Schema, _key: Option[String] = None,  _id: Option[String] = None, _rev: Option[String] = None) extends DocumentOption

//  implicit val encoder = io.circe.generic.encoding.DerivedObjectEncoder.deriveEncoder[Operation]

  case class DataSource(`type`: String, path: String, _key: Option[String] = None, _rev: Option[String] = None, _id: Option[String] = None) extends DocumentOption
  case class Attribute(name: String, dataTypeId: String)

  case class ProgressOf(_from: String, _to: String,_key: Option[String] = None,  _id: Option[String] = None, _rev: Option[String] = None) extends Edge with DocumentOption
  case class Follows(_from: String, _to: String,_key: Option[String] = None,  _id: Option[String] = None, _rev: Option[String] = None) extends Edge with DocumentOption
  case class ReadsFrom( _from: String, _to: String, _key: Option[String] = None,  _id: Option[String] = None, _rev: Option[String] = None) extends Edge with DocumentOption
  case class WritesTo(_from: String, _to: String,  _key: Option[String] = None, _id: Option[String] = None,  _rev: Option[String] = None) extends Edge with DocumentOption
  case class Implements(_from: String, _to: String, _key: Option[String] = None,  _id: Option[String] = None,  _rev: Option[String] = None) extends Edge with DocumentOption
//}


object Database extends Graph("lineages") {
  val progress: VertexCollection[Progress] = vertex[Progress]("progress")
  val execution: VertexCollection[Execution] = vertex[Execution]("execution")
  val operation: VertexCollection[Operation] = {

    import io.circe.{Decoder, Encoder}
    import io.circe.generic.semiauto._
    import com.outr.arango.rest

    new VertexCollection[Operation](this, "operation") {
      implicit val attrDec: Decoder[Attribute] = io.circe.generic.semiauto.deriveDecoder[Attribute]
      implicit val schemaDec: Decoder[Schema] = io.circe.generic.semiauto.deriveDecoder[Schema]
      implicit val attrEnc: Encoder[Attribute] = io.circe.generic.semiauto.deriveEncoder[Attribute]
      implicit val schemaEnc: Encoder[Schema] = io.circe.generic.semiauto.deriveEncoder[Schema]
      override implicit val encoder: Encoder[Operation] = deriveEncoder[Operation]
      override implicit val decoder: Decoder[Operation] = deriveDecoder[Operation]
      override protected def updateDocument(document: Operation, info: rest.CreateInfo): Operation = {
        document.copy(_key = Option(info._key), _id = Option(info._id), _rev = Option(info._rev))
      }
    }
  }

  val dataSource: VertexCollection[DataSource] = vertex[DataSource]("dataSource")

  val progressOf: EdgeCollection[ProgressOf] = edge[ProgressOf]("progressOf", ("progress", "execution"))
  val follows: EdgeCollection[Follows] = edge[Follows]("follows", ("operation", "operation"))
  val readsFrom: EdgeCollection[ReadsFrom] = edge[ReadsFrom]("readsFrom", ("operation", "dataSource"))
  val writesTo: EdgeCollection[WritesTo] = edge[WritesTo]("writesTo", ("operation", "dataSource"))
  val implements: EdgeCollection[Implements] = edge[Implements]("implements", ("execution", "operation"))
}

class ScarangoTest extends FunSpec with Matchers with MockitoSugar {

  describe("scarango") {
    it("funspec") {
      val result = Await.result(Database.init(), Duration.Inf)
//      println("Init result: " + result)
//      val execution: Execution = Await.result(Database.execution.insert(Execution("appId1", "appName1", "2.2", System.currentTimeMillis)), Duration.Inf)
//      val progress: Progress = Await.result(Database.progress.insert(Progress(System.currentTimeMillis, 10)), Duration.Inf)
//      val progressOf = Await.result(Database.progressOf.insert(ProgressOf(progress._id.get, execution._id.get)), Duration.Inf)
//      val query = aql"FOR f IN progress RETURN f"
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

    val operations = dataLineage.operations.map(op => {
      val outputSchema = findOutputSchema(dataLineage, op)
      Operation(op.mainProps.name, reflectionToString(op), outputSchema, Some(op.mainProps.output.toString))
    })
    operations.foreach(op => awaitForever(Database.operation.upsert(op)))

    dataLineage.operations.foreach(op => {
      op.mainProps.inputs
        .map(mdid => Follows("operation/" + op.mainProps.output.toString, "operation/" + mdid.toString))
        .foreach(f => awaitForever(Database.follows.insert(f)))
    })
//
//    val dataSources = dataLineage.operations.flatMap(op => op match {
//      case r: Read => r.sources.map(s => DataSource(r.sourceType, s.path))
//      case w: Write => Seq(DataSource(w.destinationType, w.path))
//    })
//    dataSources.foreach(Database.dataSource.insert(_))
//
    val execution = Execution(dataLineage.appId, dataLineage.appName, dataLineage.sparkVer, dataLineage.timestamp, Some(dataLineage.id.toString))
    Await.result(Database.execution.upsert(execution), Duration.Inf)
//  progress for batch need to be generated during migration
    val progress = dataLineage.operations.find(_.isInstanceOf[BatchWrite]).map(_ => Progress(dataLineage.timestamp, -1, Some(dataLineage.rootDataset.id.toString)))
    progress.foreach(Database.progress.insert(_))

    val progressOf = progress.map(p => ProgressOf("progress/" + p._key.get, "execution/" +  execution._key.get, p._key))
    progressOf.foreach(Database.progressOf.insert(_))

    // TODO implements
    val implements = Implements("execution/" + execution._key.get, "operation/" + dataLineage.rootOperation.mainProps.output.toString, execution._key)
    Await.result(Database.implements.insert(implements), Duration.Inf)

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

