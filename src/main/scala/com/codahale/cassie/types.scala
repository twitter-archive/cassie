package com.codahale.cassie

import types._

/**
 * Implicit conversions for all of Cassie's special types.
 *
 * @author coda
 */
package object types {
  implicit def String2AsciiString(value: String): AsciiString = AsciiString(value)
  implicit def AsciiString2String(asciiString: AsciiString): String = asciiString.value

  implicit def ByteArray2Base64ByteArray(value: Array[Byte]): Base64ByteArray = Base64ByteArray(value)
  implicit def Base64ByteArray2ByteArray(base64: Base64ByteArray): Array[Byte] = base64.value

  implicit def Int2FixedInt(value: Int): FixedInt = FixedInt(value)
  implicit def FixedInt2Int(fixedInt: FixedInt): Int = fixedInt.value

  implicit def Long2FixedLong(value: Long): FixedLong = FixedLong(value)
  implicit def FixedLong2Long(fixedLong: FixedLong): Long = fixedLong.value

  implicit def ByteArray2HexByteArray(value: Array[Byte]): HexByteArray = HexByteArray(value)
  implicit def HexByteArray2ByteArray(hex: HexByteArray): Array[Byte] = hex.value

  implicit def Int2FVarInt(value: Int): VarInt = VarInt(value)
  implicit def VarInt2Int(varInt: VarInt): Int = varInt.value

  implicit def Long2VarLong(value: Long): VarLong = VarLong(value)
  implicit def VarLong2Long(varLong: VarLong): Long = varLong.value
}
