/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime.expression

import com.didichuxing.horoscope.core.FlowDslMessage
import com.didichuxing.horoscope.runtime._

import scala.util.Try

class ExpressionBuilder(namespace: String, builtin: BuiltIn) extends ExpressionFactory with Operators {

  import FlowDslMessage.ExprDef.ExpressionCase._

  import scala.collection.JavaConversions._

  define(REFERENCE, _.getReference).using { _ =>
    definition => Reference(definition)
  }

  define(LITERAL, _.getLiteral).function(_ => Nil) { literal =>
    import FlowDslMessage.Literal.PrimitiveCase._

    val value = literal.getPrimitiveCase match {
      case PRIMITIVE_NOT_SET => NULL
      case BOOLEAN_VALUE => Value(literal.getBooleanValue)
      case NUMBER_VALUE => NumberValue(BigDecimal(literal.getNumberValue))
      case STRING_VALUE => Value(literal.getStringValue)
    }

    _ => value
  }

  define(CALL, _.getCall).function(_.getArgumentList) { call =>
    val function: BuiltIn.FuncImpl = builtin.getFunction(namespace, call.getFunction)
      .getOrElse((_: ValueList) => throw new NotImplementedError(s"no ${call.getFunction} defined in builtin"))
    children => function(children)
  }

  define(APPLY, _.getApply).method[Value](_.getFrom, _.getArgumentList) { apply =>
    val method: BuiltIn.MethodImpl = builtin.getMethod(namespace, apply.getMethod)
      .getOrElse((_, _) => throw new NotImplementedError(s"no ${apply.getMethod} defined in builtin"))
    (value, args) => method(value, args)
  }

  define(LIST_CONSTRUCTOR, _.getListConstructor).function(_.getElementList) { _ =>
    children => children
  }

  define(DICT_CONSTRUCTOR, _.getDictConstructor).function(_.getElementList.map(_.getChild)) { ctor =>
    val names = ctor.getElementList.map(_.getName)
    list => new SimpleDict(names, list.children)
  }

  define(AT, _.getAt).method[Document](_.getFrom, _.getIndex :: Nil) { _ =>
    (doc, args) => {
      val index = args.children.head
      val result = doc match {
        case table: TableView =>
          table.at(index)
        case list: ValueList =>
          list.at(
            Try(index.as[Int]).getOrElse(throw new IllegalArgumentException(
              s"list should use integer index, but ${index.valueType} is given"
            ))
          )
        case dict: ValueDict =>
          dict.at(
            Try(index.as[String]).getOrElse(throw new IllegalArgumentException(
              s"dict should use string index, but ${index.valueType} is given"
            ))
          )
      }
      if (result.nonEmpty) {
        result.get
      } else {
        throw new IndexOutOfBoundsException(s"document does not contain index $index")
      }
    }
  }

  define(VISIT, _.getVisit).method[Value](_.getFrom) { visit =>
    if (visit.hasName) {
      val name = visit.getName
      (value, _) => {
        value match {
          case doc: Document => doc.visit(name)
          case _ => NULL
        }
      }
    } else {
      (value, _) => {
        value match {
          case doc: Document => doc.visit(*)
          case _ => NULL
        }
      }
    }
  }

  define(SEARCH, _.getSearch).method[Value](_.getFrom) { search =>
    if (search.hasName) {
      val name = search.getName
      (value, _) => {
        value match {
          case doc: Document => doc.search(name)
          case _ => NULL
        }
      }
    } else {
      (value, _) => {
        value match {
          case doc: Document => doc.search(*)
          case _ => NULL
        }
      }
    }
  }

  define(SLICE, _.getSlice).method[Document](_.getFrom, m => m.getBegin :: m.getEnd :: Nil) { _ =>
    (doc, args) => {
      val (begin, end) = args.as[(Value, Value)]
      doc.table.slice(begin, end)
    }
  }

  define(SELECT, _.getSelect).method[Document](_.getFrom, _.getIndexList) { select =>
    if (select.getIndexCount == 0) {
      (doc, _) => doc.table.select(*)
    } else {
      (doc, args) => doc.table.select(args.children)
    }
  }

  define(PROJECT, _.getProject).monad(_.getFrom, _.getIndex) { _ =>
    (doc, project) => doc.table.project(project)
  }

  define(EXPAND, _.getExpand).monad(_.getFrom, _.getIndex) { _ =>
    (doc, expand) => doc.table.expand(expand)
  }

  define(QUERY, _.getQuery).monad(_.getFrom, _.getPredicate) { _ =>
    (doc, predicate) => doc.table.query(predicate)
  }

  define(FOLD, _.getFold).method[TableView](_.getFrom) { _ =>
    (table, _) => table.value
  }

  define(TRANSPOSE, _.getTranspose).method[TableView](_.getFrom) { _ =>
    (table, _) => table.transpose()
  }

  define(TRANSFORM, _.getTransform).monad(_.getFrom, _.getExpression) { _ =>
    (doc, func) => doc.transform(func)
  }
}

object ExpressionBuilder {
  def types(args: ValueList): String = {
    args.children.map(_.valueType).mkString("[", ", ", "]")
  }
}
