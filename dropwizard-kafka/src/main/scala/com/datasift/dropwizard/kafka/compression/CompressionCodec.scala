package com.datasift.dropwizard.kafka.compression

import kafka.common.UnknownCodecException

sealed abstract class CompressionCodec(val id: Int)
case object NoCompressionCodec extends CompressionCodec(0)
case object GZIPCompressionCodec extends CompressionCodec(1)
case object SnappyCompressionCodec extends CompressionCodec(2)

object CompressionCodec {

  def valueOf(codec: String): CompressionCodec = codec match {
    case "gz" | "gzip" | "default" | "yes" | "true" => GZIPCompressionCodec
    case "snappy" => SnappyCompressionCodec
    case "none" | "no" | "false" => NoCompressionCodec
    case _ =>
      throw new UnknownCodecException("%s is an unknown compression codec".format(codec))
  }
}
