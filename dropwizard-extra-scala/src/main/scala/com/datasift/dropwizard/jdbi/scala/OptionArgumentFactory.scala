package com.datasift.dropwizard.jdbi.scala

import org.skife.jdbi.v2.tweak.{ArgumentFactory, Argument}
import java.sql.{PreparedStatement, Types}
import org.skife.jdbi.v2.StatementContext

class OptionArgumentFactory(driver: String) extends ArgumentFactory[Option[_]] {

  def accepts(expectedType: Class[_], value: Any, ctx: StatementContext): Boolean = {
    value.isInstanceOf[Option[_]]
  }

  def build(expectedType: Class[_], value: Option[_], ctx: StatementContext): Argument = {
    driver match {
      case "com.microsoft.sqlserver.jdbc.SQLServerDriver" => new Argument {
        def apply(position: Int, statement: PreparedStatement, ctx: StatementContext) {
          statement.setObject(position, value.orNull)
        }
      }
      case _ => new Argument {
        def apply(position: Int, statement: PreparedStatement, ctx: StatementContext) {
          value match {
            case Some(value) => statement.setObject(position, value)
            case None        => statement.setNull(position, Types.OTHER)
          }
        }
      }
    }
  }
}


