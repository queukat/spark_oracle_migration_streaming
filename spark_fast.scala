import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._
import scala.language.implicitConversions


object NewSpark {

  def migrate(
               url: String,
               oracleUser: String,
               oraclePassword: String,
               tableName: String,
               owner: String,
               hivetable: String
             ): Unit = {
    val spark = SparkSession
      .builder()
      .appName("OracleToHiveMigrator")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val jdbcOptions = Map(
      "url" -> url,
      "user" -> oracleUser,
      "password" -> oraclePassword
    )

    val oracleSchema = spark
      .read
      .format("jdbc")
      .options(jdbcOptions)
      .option("dbtable", s"(SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE from ALL_TAB_COLUMNS WHERE TABLE_NAME = '$tableName')")
      .load()

    val queryColumns = oracleSchema.select("COLUMN_NAME")
    
    implicit val customEncoder = Encoders.tuple[String, String, String, String](Encoders.STRING, Encoders.STRING, Encoders.STRING, Encoders.STRING)

    val castedSchema = oracleSchema
      .select("COLUMN_NAME", "DATA_TYPE", "DATA_PRECISION", "DATA_SCALE")
      .as[(String, String, String, String)](customEncoder)
      .map { case (columnName, dataType, dataPrecision, dataScale) =>
      var hiveDataType: DataType = dataType match {
        case "VARCHAR2" => StringType
        case "DATE" => TimestampType
        case "NUMBER" =>
          if (dataPrecision == null || dataScale == null) {
            val maxLeftOfDecimal = spark.read.format("jdbc")
              .options(Map("url" -> s"$url",
                "dbtable" -> s"(select max(abs(trunc($columnName,0))) from $owner.$tableName)",
                "user" -> s"$oracleUser",
                "password" -> s"$oraclePassword")).load().first().length
            val maxRightOfDecimal = spark.read.format("jdbc")
              .options(Map("url" -> s"$url",
                "dbtable" -> s"(select max(mod($columnName, 1)) from $owner.$tableName)",
                "user" -> s"$oracleUser",
                "password" -> s"$oraclePassword")).load().first().toString.substring(3).length
            if (maxLeftOfDecimal + maxRightOfDecimal > 38) {
              StringType
            }
            else {
              DecimalType(maxLeftOfDecimal, maxRightOfDecimal)
            }
          }
          else {
            DecimalType(dataPrecision.toInt, dataScale.toInt)
          }
      }

      StructField(columnName, hiveDataType, nullable = true)
    }

    val createTableSQL = s"CREATE TABLE $hivetable ( ${castedSchema.map(field => s"${field.name} ${field.dataType.typeName}").collect().toSeq.mkString(", ")} )"
    spark.sql(createTableSQL)

    val fileIds = spark.read
      .format("jdbc")
      .options(jdbcOptions)
      .option("dbtable", s"(SELECT data_object_id,file_id, relative_fno, subobject_name, MIN(start_block_id) start_block_id, MAX(end_block_id)   end_block_id, SUM(blocks)  blocks   FROM (SELECT o.data_object_id, o.subobject_name, e.file_id, e.relative_fno, e.block_id  start_block_id, e.block_id + e.blocks - 1 end_block_id, e.blocks   FROM dba_extents e, dba_objects o, dba_tab_subpartitions tsp   WHERE o.owner = $owner AND o.object_name = $tableName AND e.owner = $owner AND e.segment_name = $tableName AND o.owner = e.owner AND o.object_name = e.segment_name AND (o.subobject_name = e.partition_name   OR (o.subobject_name IS NULL   AND e.partition_name IS NULL)) AND o.owner = tsp.table_owner(+) AND o.object_name = tsp.table_name(+) AND o.subobject_name = tsp.subpartition_name(+)) GROUP BY data_object_id, file_id, relative_fno, subobject_name ORDER BY data_object_id, file_id, relative_fno, subobject_name;)")
      .load()

    val queryDFs = fileIds.select("relative_fno", "data_object_id", "start_block_id", "end_block_id")
      .repartition(10)
      .map(row => {
        val relative_fno = row.getAs[Int]("relative_fno")
        val data_object_id = row.getAs[Int]("data_object_id")
        val start_block_id = row.getAs[Int]("start_block_id")
        val end_block_id = row.getAs[Int]("end_block_id")
        val query = s"SELECT /*+ NO_INDEX(t) */ ${queryColumns} FROM ${owner}.${tableName} WHERE ((rowid >= dbms_rowid.rowid_create(1, $data_object_id, $relative_fno, $start_block_id, 0) AND rowid <= dbms_rowid.rowid_create(1, $data_object_id, $relative_fno, $end_block_id, 32767)))"
        spark.sql(query)
      }).toDS().reduce((df1, df2) => df1.union(df2))



    queryDF.write.mode("append").insertInto(hivetable)
  }
}
