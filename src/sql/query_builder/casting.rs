use std::str::FromStr;

use indexmap::IndexMap;

use crate::server::schema;

use super::QueryBuilderError;

pub fn root_foreach_row_type(query: &gdc_rust_types::Query) -> Result<String, QueryBuilderError> {
    Ok(format!("Array(Tuple(query {}))", query_object_type(query)?))
}
pub fn root_rows_type(
    fields: &IndexMap<String, gdc_rust_types::Field>,
) -> Result<String, QueryBuilderError> {
    Ok(format!("Array({})", rows_object_type(fields)?))
}
pub fn root_aggregates_type(
    aggregates: &IndexMap<String, gdc_rust_types::Aggregate>,
) -> Result<String, QueryBuilderError> {
    aggregates_object_type(aggregates)
}

fn query_object_type(query: &gdc_rust_types::Query) -> Result<String, QueryBuilderError> {
    Ok(match (&query.fields, &query.aggregates) {
        (None, None) => "Map(Nothing, Nothing)".to_owned(),
        (Some(fields), None) => {
            let fields_type = rows_object_type(fields)?;
            format!("Tuple(rows Array({}))", fields_type)
        }
        (None, Some(aggregates)) => {
            let aggregates_type = aggregates_object_type(aggregates)?;
            format!("Tuple(aggregates {})", aggregates_type)
        }
        (Some(fields), Some(aggregates)) => {
            let fields_type = rows_object_type(fields)?;
            let aggregates_type = aggregates_object_type(aggregates)?;
            format!(
                "Tuple(rows Array({}), aggregates {})",
                fields_type, aggregates_type
            )
        }
    })
}
fn rows_object_type(
    fields: &IndexMap<String, gdc_rust_types::Field>,
) -> Result<String, QueryBuilderError> {
    Ok(if fields.is_empty() {
        "Map(Nothing, Nothing)".to_string()
    } else {
        let field_types = fields
            .iter()
            .map(|(column_name, field)| {
                let field_type = match field {
                    gdc_rust_types::Field::Column {
                        column: _,
                        column_type,
                    } => type_cast_string(column_type)?,
                    gdc_rust_types::Field::Relationship {
                        query,
                        relationship: _,
                    } => query_object_type(query)?,
                    gdc_rust_types::Field::Object { .. } => {
                        return Err(QueryBuilderError::Internal(
                            "Object fields not supported".to_string(),
                        ))
                    }
                    gdc_rust_types::Field::Array { .. } => {
                        return Err(QueryBuilderError::Internal(
                            "Array fields not supported".to_string(),
                        ))
                    }
                };
                Ok(format!("\"{}\" {}", column_name, field_type))
            })
            .collect::<Result<Vec<_>, _>>()?;
        format!("Tuple({})", field_types.join(", "))
    })
}
fn aggregates_object_type(
    aggregates: &IndexMap<String, gdc_rust_types::Aggregate>,
) -> Result<String, QueryBuilderError> {
    Ok(if aggregates.is_empty() {
        "Map(Nothing, Nothing)".to_string()
    } else {
        let aggregates_types = aggregates
            .iter()
            .map(|(column_name, aggregate)| {
                let aggregate_type = match aggregate {
                    // note! casting from UInt64 to UInt32 here
                    // UInt64 is serialized as a JSON string, but test suite expects JSON numbers
                    // todo: once we are able to specify return type for these aggregates, update this cast to the correct type
                    gdc_rust_types::Aggregate::ColumnCount { .. } => "UInt32".to_owned(),
                    gdc_rust_types::Aggregate::StarCount {} => "UInt32".to_owned(),
                    gdc_rust_types::Aggregate::SingleColumn { result_type, .. } => {
                        type_cast_string(result_type)?
                    }
                };
                Ok(format!("\"{}\" {}", column_name, aggregate_type))
            })
            .collect::<Result<Vec<_>, _>>()?;
        format!("Tuple({})", aggregates_types.join(", "))
    })
}
/// given a scalar type, return the type for the variant of this type that is nullable
/// used when casting rows to named tuples, which is later used to cast to JSON
/// we always wrap the type name in Nullable() as we don't know if the underlying column is nulable or not
fn type_cast_string(scalar_type: &gdc_rust_types::ScalarType) -> Result<String, QueryBuilderError> {
    let scalar_type = schema::ScalarType::from_str(scalar_type)
        .map_err(|_err| QueryBuilderError::UnknownScalarType(scalar_type.to_owned()))?;
    use schema::ScalarType as ST;
    Ok(match scalar_type {
        ST::Bool => "Nullable(Bool)",
        ST::String => "Nullable(String)",
        ST::FixedString => "Nullable(FixedString)",
        ST::UInt8 => "Nullable(UInt8)",
        ST::UInt16 => "Nullable(UInt16)",
        ST::UInt32 => "Nullable(UInt32)",
        ST::UInt64 => "Nullable(UInt64)",
        ST::UInt128 => "Nullable(UInt128)",
        ST::UInt256 => "Nullable(UInt256)",
        ST::Int8 => "Nullable(Int8)",
        ST::Int16 => "Nullable(Int16)",
        ST::Int32 => "Nullable(Int32)",
        ST::Int64 => "Nullable(Int64)",
        ST::Int128 => "Nullable(Int128)",
        ST::Int256 => "Nullable(Int256)",
        ST::Float32 => "Nullable(Float32)",
        ST::Float64 => "Nullable(Float64)",
        // casting decimal to string. Not sure if this is correct.
        // cannot cast to decimal without making a call on precision and scale
        // could go for max precision, but impossible to know scale
        ST::Decimal => "Nullable(String)",
        ST::Date => "Nullable(Date)",
        ST::Date32 => "Nullable(Date32)",
        ST::DateTime => "Nullable(DateTime)",
        ST::DateTime64 => "Nullable(DateTime64(9))",
        ST::Json => "Nullable(JSON)",
        ST::Uuid => "Nullable(UUID)",
        ST::IPv4 => "Nullable(IPv4)",
        ST::IPv6 => "Nullable(IPv6)",
        ST::Unknown => "Nullable(String)",
        // AggregateFunction types are not really meant to be looked at directly, casting to string for now
        ST::AvgUInt8 => "Nullable(String)",
        ST::AvgUInt16 => "Nullable(String)",
        ST::AvgUInt32 => "Nullable(String)",
        ST::AvgUInt64 => "Nullable(String)",
        ST::AvgUInt128 => "Nullable(String)",
        ST::AvgUInt256 => "Nullable(String)",
        ST::AvgInt8 => "Nullable(String)",
        ST::AvgInt16 => "Nullable(String)",
        ST::AvgInt32 => "Nullable(String)",
        ST::AvgInt64 => "Nullable(String)",
        ST::AvgInt128 => "Nullable(String)",
        ST::AvgInt256 => "Nullable(String)",
        ST::AvgFloat32 => "Nullable(String)",
        ST::AvgFloat64 => "Nullable(String)",
        ST::AvgDecimal => "Nullable(String)",
        ST::SumUInt8 => "Nullable(String)",
        ST::SumUInt16 => "Nullable(String)",
        ST::SumUInt32 => "Nullable(String)",
        ST::SumUInt64 => "Nullable(String)",
        ST::SumUInt128 => "Nullable(String)",
        ST::SumUInt256 => "Nullable(String)",
        ST::SumInt8 => "Nullable(String)",
        ST::SumInt16 => "Nullable(String)",
        ST::SumInt32 => "Nullable(String)",
        ST::SumInt64 => "Nullable(String)",
        ST::SumInt128 => "Nullable(String)",
        ST::SumInt256 => "Nullable(String)",
        ST::SumFloat32 => "Nullable(String)",
        ST::SumFloat64 => "Nullable(String)",
        ST::SumDecimal => "Nullable(String)",
        ST::MaxUInt8 => "Nullable(String)",
        ST::MaxUInt16 => "Nullable(String)",
        ST::MaxUInt32 => "Nullable(String)",
        ST::MaxUInt64 => "Nullable(String)",
        ST::MaxUInt128 => "Nullable(String)",
        ST::MaxUInt256 => "Nullable(String)",
        ST::MaxInt8 => "Nullable(String)",
        ST::MaxInt16 => "Nullable(String)",
        ST::MaxInt32 => "Nullable(String)",
        ST::MaxInt64 => "Nullable(String)",
        ST::MaxInt128 => "Nullable(String)",
        ST::MaxInt256 => "Nullable(String)",
        ST::MaxFloat32 => "Nullable(String)",
        ST::MaxFloat64 => "Nullable(String)",
        ST::MaxDecimal => "Nullable(String)",
        ST::MinUInt8 => "Nullable(String)",
        ST::MinUInt16 => "Nullable(String)",
        ST::MinUInt32 => "Nullable(String)",
        ST::MinUInt64 => "Nullable(String)",
        ST::MinUInt128 => "Nullable(String)",
        ST::MinUInt256 => "Nullable(String)",
        ST::MinInt8 => "Nullable(String)",
        ST::MinInt16 => "Nullable(String)",
        ST::MinInt32 => "Nullable(String)",
        ST::MinInt64 => "Nullable(String)",
        ST::MinInt128 => "Nullable(String)",
        ST::MinInt256 => "Nullable(String)",
        ST::MinFloat32 => "Nullable(String)",
        ST::MinFloat64 => "Nullable(String)",
        ST::MinDecimal => "Nullable(String)",
        ST::MaxDate => "Nullable(String)",
        ST::MaxDate32 => "Nullable(String)",
        ST::MaxDateTime => "Nullable(String)",
        ST::MaxDateTime64 => "Nullable(String)",
        ST::MinDate => "Nullable(String)",
        ST::MinDate32 => "Nullable(String)",
        ST::MinDateTime => "Nullable(String)",
        ST::MinDateTime64 => "Nullable(String)",
    }
    .to_owned())
}
