/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.data.iceberg.parquet

import io.airbyte.cdk.load.data.AirbyteType
import io.airbyte.cdk.load.data.ArrayType
import io.airbyte.cdk.load.data.ArrayTypeWithoutSchema
import io.airbyte.cdk.load.data.BooleanType
import io.airbyte.cdk.load.data.DateType
import io.airbyte.cdk.load.data.IntegerType
import io.airbyte.cdk.load.data.NumberType
import io.airbyte.cdk.load.data.ObjectType
import io.airbyte.cdk.load.data.ObjectTypeWithEmptySchema
import io.airbyte.cdk.load.data.ObjectTypeWithoutSchema
import io.airbyte.cdk.load.data.StringType
import io.airbyte.cdk.load.data.TimeTypeWithTimezone
import io.airbyte.cdk.load.data.TimeTypeWithoutTimezone
import io.airbyte.cdk.load.data.TimestampTypeWithTimezone
import io.airbyte.cdk.load.data.TimestampTypeWithoutTimezone
import io.airbyte.cdk.load.data.UnionType
import io.airbyte.cdk.load.data.UnknownType
import java.util.UUID
import org.apache.iceberg.Schema
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Types
import org.apache.iceberg.types.Types.NestedField

class AirbyteTypeToIcebergSchema {

    fun convert(airbyteSchema: AirbyteType): Type {
        return when (airbyteSchema) {
            is ObjectType -> {
                Types.StructType.of(
                    *airbyteSchema.properties.entries
                        .map { (name, field) ->
                            if (field.nullable) {
                                NestedField.optional(
                                    UUID.randomUUID().hashCode(),
                                    name,
                                    convert(field.type)
                                )
                            } else {
                                NestedField.required(
                                    UUID.randomUUID().hashCode(),
                                    name,
                                    convert(field.type)
                                )
                            }
                        }
                        .toTypedArray()
                )
            }
            is ArrayType -> {
                val convert = convert(airbyteSchema.items.type)
                if (airbyteSchema.items.nullable) {
                    return Types.ListType.ofOptional(UUID.randomUUID().hashCode(), convert)
                }
                return Types.ListType.ofRequired(UUID.randomUUID().hashCode(), convert)
            }
            is ArrayTypeWithoutSchema ->
                throw IllegalArgumentException("Array type without schema is not supported")
            is BooleanType -> Types.BooleanType.get()
            is DateType -> Types.DateType.get()
            is IntegerType -> Types.LongType.get()
            is NumberType -> Types.DoubleType.get()
            is ObjectTypeWithEmptySchema ->
                throw IllegalArgumentException("Object type with empty schema is not supported")
            is ObjectTypeWithoutSchema ->
                throw IllegalArgumentException("Object type without schema is not supported")
            is StringType -> Types.StringType.get()
            is TimeTypeWithTimezone,
            is TimeTypeWithoutTimezone -> Types.TimeType.get()
            is TimestampTypeWithTimezone -> Types.TimestampType.withZone()
            is TimestampTypeWithoutTimezone -> Types.TimestampType.withoutZone()
            is UnionType -> {
                if (airbyteSchema.options.size == 1) {
                    return Types.ListType.ofOptional(
                        UUID.randomUUID().hashCode(),
                        convert(airbyteSchema.options.first())
                    )
                }
                // Iceberg doesnt support a UNION data type
                return Types.ListType.ofOptional(
                    UUID.randomUUID().hashCode(),
                    Types.StringType.get()
                )
            }
            is UnknownType -> Types.StringType.get()
        }
    }
}

fun ObjectType.toIcebergSchema(primaryKeys: List<List<String>>): Schema {
    val mutableListOf = mutableListOf<NestedField>()
    val identifierFields = mutableSetOf<Int>()
    val identifierFieldNames = primaryKeys.flatten().toSet()
    val icebergTypeConverter = AirbyteTypeToIcebergSchema()
    this.properties.entries.forEach { (name, field) ->
        val id = generatedSchemaFieldId()
        mutableListOf.add(
            NestedField.of(
                id,
                field.nullable,
                name,
                icebergTypeConverter.convert(field.type),
            ),
        )
        if (identifierFieldNames.contains(name)) {
            identifierFields.add(id)
        }
    }
    return Schema(mutableListOf, identifierFields)
}

private fun generatedSchemaFieldId() = UUID.randomUUID().hashCode()
