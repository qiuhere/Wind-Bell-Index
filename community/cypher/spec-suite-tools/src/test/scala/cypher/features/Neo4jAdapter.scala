/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cypher.features

import java.lang.Boolean.TRUE

import cypher.features.Neo4jExceptionToExecutionFailed.convert
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.cypher_hints_error
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.graphdb.config.Setting
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade
import org.neo4j.test.TestDatabaseManagementServiceBuilder
import org.opencypher.tools.tck.api.ExecQuery
import org.opencypher.tools.tck.api.Graph
import org.opencypher.tools.tck.api.QueryType
import org.opencypher.tools.tck.api.StringRecords
import org.opencypher.tools.tck.values.CypherValue

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable.ArrayBuffer
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object Neo4jAdapter {
  val defaultTestConfig: collection.Map[Setting[_], Object] = Map[Setting[_], Object](cypher_hints_error -> TRUE)

  def apply(executionPrefix: String, graphDatabaseFactory: TestDatabaseManagementServiceBuilder,
            dbConfig: collection.Map[Setting[_], Object]): Neo4jAdapter = {
    val managementService = createManagementService(dbConfig, graphDatabaseFactory)
    new Neo4jAdapter(managementService, executionPrefix)
  }

  private def createManagementService(config: collection.Map[Setting[_], Object], graphDatabaseFactory: TestDatabaseManagementServiceBuilder) = {
    graphDatabaseFactory.impermanent().setConfig(config.asJava).build()
  }
}

class Neo4jAdapter(var managementService: DatabaseManagementService,
                   executionPrefix: String) extends Graph with Neo4jProcedureAdapter {

  protected var database: GraphDatabaseFacade =
    new GraphDatabaseCypherService(managementService.database(DEFAULT_DATABASE_NAME)).getGraphDatabaseService

  private val explainPrefix = "EXPLAIN\n"

  override def cypher(query: String, params: Map[String, CypherValue], meta: QueryType): Result = {
    val neo4jParams = params.mapValues(v => TCKValueToNeo4jValue(v)).asJava

    var tx = database.beginTx
    val queryToExecute = if (meta == ExecQuery) {
      s"$executionPrefix $query"
    } else query
    val result: Result = Try(tx.execute(queryToExecute, neo4jParams)).flatMap(r => Try(convertResult(r))) match {
      case Success(converted) =>
        Try(tx.commit()) match {
          case Failure(exception) =>
            convert(Phase.runtime, exception)
          case Success(_) => converted
        }
      case Failure(exception) =>
        tx.close()
        tx = database.beginTx()
        val explainedResult = Try(tx.execute(explainPrefix + queryToExecute, neo4jParams))
        val phase = explainedResult match {
          case Failure(_) => Phase.compile
          case Success(_) => Phase.runtime
        }
        Try(tx.rollback()) match {
          case Failure(exception) => convert(Phase.runtime, exception)
          case Success(_) => convert(phase, exception)
        }
    }
    Try(tx.close()) match {
      case Failure(exception) =>
        convert(Phase.runtime, exception)
      case Success(_) => result
    }
  }

  def convertResult(result: org.neo4j.graphdb.Result): Result = {
    val header = result.columns().asScala.toList
    val rows = ArrayBuffer[Map[String, String]]()
    result.accept(new ResultVisitor[RuntimeException] {
      override def visit(row: org.neo4j.graphdb.Result.ResultRow): Boolean = {
        rows.append(header.map(k => k -> Neo4jValueToString(row.get(k))).toMap)
        true
      }
    })
    StringRecords(header, rows.toList)
  }

  override def close(): Unit = {
    managementService.shutdown()
    managementService = null
    database = null
  }

}
