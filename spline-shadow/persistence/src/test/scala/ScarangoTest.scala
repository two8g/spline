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

import ArangoModel._
import com.outr.arango._
import com.outr.arango.managed._
import org.apache.commons.lang.builder.ToStringBuilder.reflectionToString
import org.scalatest.mockito.MockitoSugar
import org.scalatest.tools.Durations
import org.scalatest.{FunSpec, Matchers}
import za.co.absa.spline.{model => splinemodel}
import za.co.absa.spline.model._
import za.co.absa.spline.model.dt.Simple
import za.co.absa.spline.model.op.{Operation => _, _}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

object ArangoModel {
  case class Progress(timestamp: Long, readCount: Long, _key: Option[String] = None, _id: Option[String] = None,  _rev: Option[String] = None) extends DocumentOption
  case class Execution(appId: String, appName: String, sparkVer: String, timestamp: Long, _key: Option[String] = None, _id: Option[String] = None, _rev: Option[String] = None) extends DocumentOption
  case class Operation(name: String, expression: String, outputSchema: Seq[String], _key: Option[String] = None,  _id: Option[String] = None, _rev: Option[String] = None) extends DocumentOption
  case class DataSource(`type`: String, path: String, _key: Option[String] = None, _rev: Option[String] = None, _id: Option[String] = None) extends DocumentOption

  case class ProgressOf(_from: String, _to: String,_key: Option[String] = None,  _id: Option[String] = None, _rev: Option[String] = None) extends Edge with DocumentOption
  case class Follows(_from: String, _to: String,_key: Option[String] = None,  _id: Option[String] = None, _rev: Option[String] = None) extends Edge with DocumentOption
  case class ReadsFrom( _from: String, _to: String, _key: Option[String] = None,  _id: Option[String] = None, _rev: Option[String] = None) extends Edge with DocumentOption
  case class WritesTo(_from: String, _to: String,  _key: Option[String] = None, _id: Option[String] = None,  _rev: Option[String] = None) extends Edge with DocumentOption
  case class Implements(_from: String, _to: String, _key: Option[String] = None,  _id: Option[String] = None,  _rev: Option[String] = None) extends Edge with DocumentOption
}

class ScarangoTest extends FunSpec with Matchers with MockitoSugar {

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
  }

  describe("scarango") {
    it("funspec") {
      val result = Await.result(Database.init(), Duration.Inf)
//      println("Init result: " + result)
//      val execution: Execution = Await.result(Database.execution.insert(Execution("appId1", "appName1", "2.2", System.currentTimeMillis)), Duration.Inf)
//      val progress: Progress = Await.result(Database.progress.insert(Progress(System.currentTimeMillis, 10)), Duration.Inf)
//      val progressOf = Await.result(Database.progressOf.insert(ProgressOf(progress._id.get, execution._id.get)), Duration.Inf)
      val query = aql"FOR f IN progress RETURN f"
      save(datalineage())
    }

  }

  // TODO store full attribute information
  private def findOutputSchema(dataLineage: DataLineage, operation: splinemodel.op.Operation) : Seq[String] = {
    val metaDataset: MetaDataset = dataLineage.datasets.find((dts: MetaDataset) => dts.id == operation.mainProps.output).get
    metaDataset.schema.attrs.map(attrId => {
      dataLineage.attributes.find(_.id == attrId).get.name
    })
  }

  private def awaitForever(future: Future[_]) = {
    Await.result(future, Duration.Inf)
  }

  def save(dataLineage: DataLineage): Unit = {
//    val execution = Execution(dataLineage.appId, dataLineage.appName, dataLineage.sparkVer, dataLineage.timestamp, Some(dataLineage.id.toString))
//    Database.execution.upsert(execution)

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
//    // progress for batch need to be generated during migration
//    val progress = dataLineage.operations.find(_.isInstanceOf[BatchWrite]).map(_ => Progress(dataLineage.timestamp, -1, Some(dataLineage.rootDataset.id.toString)))
//    progress.foreach(Database.progress.insert(_))
//
//    val progressOf = progress.map(p => ProgressOf(execution._key.get, p._key.get, p._key))
//    progressOf.foreach(Database.progressOf.insert(_))
//
//    // TODO implements
//    val implements = Implements(execution._key.get, dataLineage.rootOperation.mainProps.output.toString)
//    Await.result(Database.implements.insert(implements), Duration.Inf)

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
        Attribute(randomUUID(), "_1", dataTypes.head.id),
        Attribute(randomUUID(), "_2", dataTypes.head.id),
        Attribute(randomUUID(), "_3", dataTypes.head.id)
      )
      val aSchema = Schema(attributes.map(_.id))
      val bSchema = Schema(attributes.map(_.id).tail)

      val md1 = MetaDataset(datasetId, aSchema)
      val md2 = MetaDataset(randomUUID, aSchema)
      val md3 = MetaDataset(randomUUID, bSchema)
      val md4 = MetaDataset(randomUUID, bSchema)

      DataLineage(
        appId,
        appName,
        timestamp,
        "2.3.0",
        Seq(
          BatchWrite(OperationProps(randomUUID, "Write", Seq(md1.id), md1.id), "parquet", path, append),
          Generic(OperationProps(randomUUID, "Union", Seq(md1.id, md2.id), md3.id), "rawString1"),
          Generic(OperationProps(randomUUID, "Filter", Seq(md4.id), md2.id), "rawString2"),
          Generic(OperationProps(randomUUID, "LogicalRDD", Seq.empty, md4.id), "rawString3"),
          Generic(OperationProps(randomUUID, "Filter", Seq(md4.id), md1.id), "rawString4")
        ),
        Seq(md1, md2, md3, md4),
        attributes,
        dataTypes
      )
    }

}

