package com.outr.arango

import splinescarango.{Attribute, DataType, Schema}

/**
  * Provides encoders for subtypes of vertex types needed for encoder generation to work.
  * This is rather a hack.
  * Salat is able generate encoders without having to do below.
  */
package object managed {

  import io.circe.{Decoder, Encoder}
  import io.circe.generic.semiauto._
  implicit val attrDec: Decoder[Attribute] = deriveDecoder[Attribute]
  implicit val schemaDec: Decoder[Schema] = deriveDecoder[Schema]
  implicit val attrEnc: Encoder[Attribute] = deriveEncoder[Attribute]
  implicit val schemaEnc: Encoder[Schema] = deriveEncoder[Schema]
  implicit val dataTypeDec: Decoder[DataType] = deriveDecoder[DataType]
  implicit val dataTypeEnc: Encoder[DataType] = deriveEncoder[DataType]

}

