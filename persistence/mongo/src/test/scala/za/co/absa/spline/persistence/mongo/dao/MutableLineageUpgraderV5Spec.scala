package za.co.absa.spline.persistence.mongo.dao

import com.mongodb.DBObject
import com.mongodb.casbah.commons.{MongoDBList, MongoDBObject => DBO}
import java.{util => ju}

import org.scalatest.FunSpec
import MutableLineageUpgraderV5._
import org.scalatest.Matchers.convertToStringShouldWrapper
import salat.{BinaryTypeHintStrategy, TypeHintFrequency}
import za.co.absa.spline.persistence.mongo.dao.LineageDAOv5.Field
import scala.collection.JavaConverters._

class MutableLineageUpgraderV5Spec extends FunSpec {

  val hints = BinaryTypeHintStrategy(TypeHintFrequency.Always)

  describe("type hints upgraded") {
    it("read type hint upgraded") {
      val dbo = DBO("operations" → List(
        DBO(Field.t → hints.encode("za.co.absa.spline.model.op.Read"))
      ).asJava)
      upgradeLineage(dbo)
      val updated = dbo.get("operations").asInstanceOf[ju.List[DBObject]].asScala.head.get(Field.t)
      hints.decode(updated) shouldBe "za.co.absa.spline.model.op.BatchRead"
    }
  }

}
