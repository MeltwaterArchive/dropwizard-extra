package com.datasift.dropwizard.hbase.config

import com.yammer.dropwizard.config.Configuration
import org.hbase.async.Scanner
import reflect.BeanProperty

/**Configuration for a [[com.datasift.dropwizard.hbase.scanner.RowScanner]]
 *
 * This isn't generally designed to be defined in a configuration file as many
 * of the properties will need to be set dynamically. Its purpose is to
 * encapsulate all the configuration for a [[org.hbase.async.Scanner]] to be
 * used on initialization.
 */
class ScannerConfiguration(

  @BeanProperty
  var family: Option[Array[Byte]] = None,

  @BeanProperty
  var qualifier: Option[Array[Byte]] = None,

  @BeanProperty
  var maxTimestamp: Long = 0,

  @BeanProperty
  var minTimestamp: Long = Long.MaxValue,

  @BeanProperty
  var keyRegexp: Option[String] = None,

  @BeanProperty
  var maxKeyValues: Int = Scanner.DEFAULT_MAX_NUM_KVS,

  @BeanProperty
  var maxRows: Int = Scanner.DEFAULT_MAX_NUM_ROWS,

  @BeanProperty
  var enableBlockCache: Boolean = true,

  @BeanProperty
  var startKey: Option[Array[Byte]] = None,

  @BeanProperty
  var stopKey: Option[Array[Byte]] = None
) extends Configuration
