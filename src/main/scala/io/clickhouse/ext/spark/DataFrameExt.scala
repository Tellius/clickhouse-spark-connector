package io.clickhouse.ext.spark

import io.clickhouse.ext.{ClickhouseClient, ClickhouseConnectionFactory}
import ru.yandex.clickhouse.ClickHouseDataSource
import io.clickhouse.ext.Utils._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

object ClickhouseSparkExt{
  implicit def extraOperations(df: org.apache.spark.sql.DataFrame) = DataFrameExt(df)
}

case class DataFrameExt(df: org.apache.spark.sql.DataFrame) extends Serializable {

  def dropClickhouseDb(dbName: String, clusterNameO: Option[String] = None)
                      (implicit ds: ClickHouseDataSource){
    val client = ClickhouseClient(clusterNameO)(ds)
    clusterNameO match {
      case None => client.dropDb(dbName)
      case Some(x) => client.dropDbCluster(dbName)
    }
  }

  def createClickhouseDb(dbName: String, clusterNameO: Option[String] = None)
                        (implicit ds: ClickHouseDataSource){
    val client = ClickhouseClient(clusterNameO)(ds)
    clusterNameO match {
      case None => client.createDb(dbName)
      case Some(x) =>
        //Create Db on master
        client.createDb(dbName)
        //Create Db on slaves
        client.createDbCluster(dbName)
    }
  }

  def createClickhouseTable(dbName: String, tableName: String, clusterNameO: Option[String] = None, partitionColumnName: Option[String] = None, indexColumns: Option[Seq[String]] = None,primaryKeyColumnOption:Option[String]=None,isReplicationNeeded:Boolean=false)
                           (implicit ds: ClickHouseDataSource){
    val client = ClickhouseClient(clusterNameO)(ds)
    val sqlStmt = createClickhouseTableDefinitionSQL(dbName, tableName, partitionColumnName, indexColumns,
      primaryKeyColumnOption, isReplicationNeeded)
    clusterNameO match {
      case None => client.query(sqlStmt)
      case Some(clusterName) =>
        //create a temporary table with replicated/local table on all nodes
        val replTableName = s"${tableName}_local"
        val replTableCreateStatement = createClickhouseTableDefinitionSQL(dbName, replTableName, 
          partitionColumnName, indexColumns, primaryKeyColumnOption, isReplicationNeeded)
        client.queryCluster(replTableCreateStatement)
        //create distrib table on all nodes
        val distributedTableCreateStatement = createClickhouseDistTableDefinitionSQL(clusterName, dbName, tableName, replTableName, partitionColumnName)//s"CREATE TABLE IF NOT EXISTS `${dbName}`.${tableName} AS ${dbName}.${replTableName} ENGINE = Distributed($clusterName, `$dbName`, $replTableName, rand());"
        client.queryCluster(distributedTableCreateStatement)
    }
  }

  def splitRDD(df:DataFrame, splitSizeOption:Option[Int]):List[RDD[Row]] = {
    val numOfPartitions = df.rdd.getNumPartitions
    splitSizeOption match {
      case Some(splitSize) => 
        val ranges = bucketise(numOfPartitions, splitSize)
        ranges.map{
          case (startIndex, endIndex) =>
            df.rdd.mapPartitionsWithIndex((index, iterator) => {
              if(index >= startIndex && index < endIndex)
                iterator
              else
                Seq[Row]().toIterator
            })
        }
      case None => List(df.rdd)

    }
  }
  def saveToClickhouse(dbName: String, tableName: String, partitionFunc: (org.apache.spark.sql.Row) => java.sql.Date, partitionColumnName: String = "mock_date", clusterNameO: Option[String] = None, batchSize: Int = 100000, maxConnections:Option[Int] = None)
                      (implicit ds: ClickHouseDataSource):Map[String,Int] = {

    val defaultHost = ds.getHost
    val defaultPort = ds.getPort

    val (clusterTableName, clickHouseHosts) = clusterNameO match {
      case Some(clusterName) =>
        // get nodes from cluster
        val client = ClickhouseClient(clusterNameO)(ds)
        (tableName, client.getClusterNodes())
      case None =>
        (tableName, Seq(defaultHost))
    }

    val schema = df.schema

    val splittedRDD = splitRDD(df, maxConnections)

    // following code is going to be run on executors
    val insertResults = splittedRDD.flatMap(split => split.mapPartitionsWithIndex((partitionIndex:Int, partition: Iterator[org.apache.spark.sql.Row])=>{

      val modulo = (partitionIndex % clickHouseHosts.length)
      val targetHost = clickHouseHosts(modulo)
      val targetHostDs = ClickhouseConnectionFactory.get(targetHost, defaultPort)

      // explicit closing
      using(targetHostDs.getConnection) { conn =>

        val insertStatementSql = generateInsertStatment(schema, dbName, clusterTableName, partitionColumnName)
        val statement = conn.prepareStatement(insertStatementSql)

        var totalInsert = 0
        var counter = 0

        while(partition.hasNext){

          counter += 1
          val row = partition.next()

          // create mock date
          val partitionVal = partitionFunc(row)
          statement.setDate(1, partitionVal)

          // map fields
          schema.foreach{ f =>
            val fieldName = f.name
            val fieldIdx = row.fieldIndex(fieldName)
            val fieldVal = row.get(fieldIdx)
            statement.setObject(fieldIdx + 2, fieldVal)
          }
          statement.addBatch()

          if(counter >= batchSize){
            val r = statement.executeBatch()
            totalInsert += r.sum
            counter = 0
          }

        } // end: while

        if(counter > 0) {
          val r = statement.executeBatch()
          totalInsert += r.sum
          counter = 0
        }

        // return: Seq((host, insertCount))
        List((targetHost, totalInsert)).toIterator
      }

    }).collect())

    insertResults.groupBy(_._1)
      .map(x => (x._1, x._2.map(_._2).sum))    

    // aggr insert results by hosts
    
  }

  def saveToClickhouse(dbName: String, tableName: String, batchSize: Int, clusterNameO:Option[String])(implicit ds: ClickHouseDataSource):Map[String,Int] = {
    saveToClickhouse(dbName, tableName, batchSize, clusterNameO, None)
  }

  def saveToClickhouse(dbName: String, tableName: String, batchSize: Int, clusterNameO:Option[String], maxConnections:Option[Int])(implicit ds: ClickHouseDataSource):Map[String,Int] = {
      
    val defaultHost = ds.getHost
    val defaultPort = ds.getPort

    val (clusterTableName, clickHouseHosts) = clusterNameO match {
      case Some(clusterName) =>
        // get nodes from cluster
        val client = ClickhouseClient(clusterNameO)(ds)
        (tableName, client.getClusterNodes())
      case None =>
        (tableName, Seq(defaultHost))
    }

    val schema = df.schema

    val splittedRDD = splitRDD(df, maxConnections)

    // following code is going to be run on executors
    val insertResults = splittedRDD.flatMap(split => split.mapPartitionsWithIndex((partitionIndex:Int, partition: Iterator[org.apache.spark.sql.Row]) => {

      val modulo = (partitionIndex % clickHouseHosts.length)
      val targetHost = clickHouseHosts(modulo)
      val targetHostDs = ClickhouseConnectionFactory.get(targetHost, defaultPort)

      // explicit closing
      using(targetHostDs.getConnection) { conn =>

        val insertStatementSql = generateInsertStatment(schema, dbName, clusterTableName)
        val statement = conn.prepareStatement(insertStatementSql)

        var totalInsert = 0
        var counter = 0

        while(partition.hasNext){

          counter += 1
          val row = partition.next()

          // map fields
          schema.foreach{ f =>
            val fieldName = f.name
            val fieldIdx = row.fieldIndex(fieldName)
            val fieldVal = row.get(fieldIdx)
            statement.setObject(fieldIdx + 1, fieldVal)
          }
          statement.addBatch()

          if(counter >= batchSize){
            val r = statement.executeBatch()
            totalInsert += r.sum
            counter = 0
          }

        } // end: while

        if(counter > 0) {
          val r = statement.executeBatch()
          totalInsert += r.sum
          counter = 0
        }

        // return: Seq((host, insertCount))
        List((targetHost, totalInsert)).toIterator
      }

    }).collect())

    // aggr insert results by hosts
    insertResults.groupBy(_._1)
      .map(x => (x._1, x._2.map(_._2).sum))
  }

   
  private def generateInsertStatment(schema: org.apache.spark.sql.types.StructType, dbName: String, tableName: String) = {
    val columns = schema.map(f => f.name).toList
    val vals = 1 to (columns.length) map (i => "?")
    s"INSERT INTO `$dbName`.$tableName (${columns.mkString(",")}) VALUES (${vals.mkString(",")})"
  }

  private def generateInsertStatment(schema: org.apache.spark.sql.types.StructType, dbName: String, tableName: String, partitionColumnName: String) = {
    val columns = partitionColumnName :: schema.map(f => f.name).toList
    val vals = 1 to (columns.length) map (i => "?")
    s"INSERT INTO `$dbName`.$tableName (${columns.mkString(",")}) VALUES (${vals.mkString(",")})"
  }

 
  //TODO: Ignored index columns as of now
  private def createClickhouseTableDefinitionSQL(dbName: String, tableName: String, partitionColumnNameOption: Option[String], indexColumnsOption: Option[Seq[String]],primaryKeyColumnOption:Option[String]=None,isReplicationNeeded:Boolean=false):String = {
    (partitionColumnNameOption, isReplicationNeeded, primaryKeyColumnOption) match {
      case (Some(partitionColumnName), true, Some(primaryKeyColumn)) =>
        createClickhouseTableDefinitionSQL(dbName, tableName, partitionColumnName,primaryKeyColumn)
      case _ =>
        createClickhouseTableDefinitionSQL(dbName, tableName)
    }
  } 
  
  private def createClickhouseTableDefinitionSQL(dbName: String, tableName: String, 
    partitionColumnName: String, primaryKeyColumn:String)= {

    val header = s"""
          CREATE TABLE IF NOT EXISTS `$dbName`.$tableName(
          """

    val columns = s"$partitionColumnName Date" :: df.schema.map{ f =>
      Seq(f.name, sparkType2ClickhouseType(f.dataType, f.nullable)).mkString(" ")
    }.toList
    val columnsStr = columns.mkString(",\n")

    val footer = s""")ENGINE = ReplicatedMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}', '{replica}')
          ORDER BY ($partitionColumnName, intHash32($primaryKeyColumn))
          SAMPLE BY intHash32($primaryKeyColumn)
          """    
    Seq(header, columnsStr, footer).mkString("\n")
  }

  private def createClickhouseTableDefinitionSQL(dbName: String, tableName: String):String = {

    val header = s"""
          CREATE TABLE IF NOT EXISTS `$dbName`.$tableName(
          """

    val columns = df.schema.map{ f =>
      Seq(f.name, sparkType2ClickhouseType(f.dataType, f.nullable)).mkString(" ")
    }.toList
    val columnsStr = columns.mkString(",\n")

    val footer = s"""
          )ENGINE = Log;
          """

    Seq(header, columnsStr, footer).mkString("\n")
  }

  private def createClickhouseDistTableDefinitionSQL(clusterName:String, dbName: String, tableName: String, replTableName:String, partitionColumnNameOption:Option[String]):String = {

    val header = s"""
          CREATE TABLE IF NOT EXISTS `$dbName`.$tableName(
          """

    val columns = partitionColumnNameOption.map(partitionColumnName => s"$partitionColumnName Date").getOrElse("") :: df.schema.map{ f =>
      Seq(f.name, sparkType2ClickhouseType(f.dataType, f.nullable)).mkString(" ")
    }.toList
    val columnsStr = columns.mkString(",\n")

    val footer = s"""
          )ENGINE = Distributed($clusterName, `$dbName`, $replTableName, rand());;
          """

    Seq(header, columnsStr, footer).mkString("\n")
  }

  private def sparkType2ClickhouseType(sparkType: org.apache.spark.sql.types.DataType,
        nullable:Boolean = false)= {
  
val clickHouseType = sparkType match {
      case LongType => "Int64"
      case DoubleType => "Float64"
      case FloatType => "Float32"
      case IntegerType => "Int32"
      case StringType => "String"
      case BooleanType => "UInt8"
      case TimestampType => "DateTime"
      case DateType => "Date"
      case NullType => "Date"
      case _ => "unknown"
    }
    if(nullable)
      s"Nullable($clickHouseType)"
    else
      clickHouseType
  }

}
