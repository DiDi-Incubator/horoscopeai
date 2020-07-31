package com.didichuxing.horoscope.util

import com.didichuxing.horoscope.util.Utils.close
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.client.{Delete, Scan, Table}
import org.apache.hadoop.hbase.filter.{ColumnPrefixFilter, KeyOnlyFilter}
import org.apache.hadoop.hbase.util.Bytes

object HBaseUtil extends Logging {

  def destroyTable(t: Table): Long = {
    var counter: Int = 0
    try {
      val scan = new Scan().setFilter(new KeyOnlyFilter())
      val iter = t.getScanner(scan).iterator()
      while (iter.hasNext) {
        val r = iter.next()
        val delete = new Delete(r.getRow)
        t.delete(delete)
        counter += 1
      }
      info(("msg", s"delete all rows ${counter}"), ("table", t.getName.getNameAsString))
    } finally {
      close(t)
    }
    counter
  }

  def destroyTable(t: Table, family: HColumnDescriptor, colName: String): Long = {
    var counter: Long = 0
    try {
      val scan = new Scan().addFamily(family.getName)
        .setFilter(new ColumnPrefixFilter(Bytes.toBytes(colName)))
      val iter = t.getScanner(scan).iterator()
      while (iter.hasNext) {
        val r = iter.next()
        val delete = new Delete(r.getRow)
        t.delete(delete)
        counter += 1
      }
      info(("msg", s"delete rows ${counter}"),
        ("table", t.getName.getNameAsString), ("family", family.getNameAsString), ("col", colName))
    } finally {
      close(t)
    }
    counter
  }

  def countTable(t: Table): Long = {
    var counter: Long = 0
    try {
      val scan = new Scan().setFilter(new KeyOnlyFilter())
      val iter = t.getScanner(scan).iterator()
      while (iter.hasNext) {
        iter.next()
        counter += 1
      }
      info(("msg", s"count rows ${counter}"), ("table", t.getName.getNameAsString))
    } finally {
      close(t)
    }
    counter
  }

  def countTable(t: Table, family: HColumnDescriptor, colName: String): Long = {
    var counter: Long = 0
    try {
      val scan = new Scan().addFamily(family.getName).setFilter(new ColumnPrefixFilter(Bytes.toBytes(colName)))
      val iter = t.getScanner(scan).iterator()
      while (iter.hasNext) {
        val result = iter.next()
        val cellScanner = result.cellScanner()
        while (cellScanner.advance()) {
          counter += 1
        }
      }
      info(("msg", s"count rows ${counter}"),
        ("table", t.getName.getNameAsString), ("family", family.getNameAsString), ("col", colName))
    } finally {
      close(t)
    }
    counter
  }

}
