import play.api.libs.json._
import scala.io.Source

object MappingJsonTest extends App {
  val mappingPath = if (args.length > 0) args(0) else "conf/mapping.json"
  val mappingSource = Source.fromFile(mappingPath)
  val mappingJson = try mappingSource.mkString finally mappingSource.close()
  val json = Json.parse(mappingJson)
  println("[TEST] Top-level keys:")
  json.as[JsObject].fields.foreach { case (tableName, columnsArr) =>
    println(s"[TEST] Table: $tableName, type: " + columnsArr.getClass.getSimpleName)
    columnsArr match {
      case arr: JsArray =>
        println(s"[TEST] Table '$tableName' is an array with length: " + arr.value.length)
        arr.value.zipWithIndex.foreach { case (colMap, idx) =>
          println(s"  [TEST]  Col $idx: " + Json.prettyPrint(colMap))
        }
      case other =>
        println(s"[ERROR] Table mapping for '$tableName' is not an array: " + Json.prettyPrint(other))
    }
  }
}