//! Version compatibility layer for different Polars versions

#[cfg(not(any(
    feature = "polars-0-40",
    feature = "polars-0-41",
    feature = "polars-0-42",
    feature = "polars-0-43",
    feature = "polars-0-44",
    feature = "polars-0-45",
    feature = "polars-0-46",
    feature = "polars-0-47",
    feature = "polars-0-48",
    feature = "polars-0-49",
    feature = "polars-0-50"
)))]
use crate::PolarsSerdeError;
use crate::Result;
use arrow::record_batch::RecordBatch;
#[cfg(any(
    feature = "polars-0-40",
    feature = "polars-0-41",
    feature = "polars-0-42",
    feature = "polars-0-43",
    feature = "polars-0-44",
    feature = "polars-0-45",
    feature = "polars-0-46",
    feature = "polars-0-47",
    feature = "polars-0-48",
    feature = "polars-0-49",
    feature = "polars-0-50"
))]
use df_interchange::Interchange;

/// Macro to generate version-specific conversion functions based on enabled features
macro_rules! impl_version_conversions {
    () => {
        /// Convert DataFrame to RecordBatch using the appropriate Polars version
        pub fn dataframe_to_arrow(mut df: polars::prelude::DataFrame) -> Result<Vec<RecordBatch>> {
            df.as_single_chunk();

            #[cfg(feature = "polars-0-40")]
            return Ok(Interchange::from_polars_0_40(df)?.to_arrow_55()?);

            #[cfg(feature = "polars-0-41")]
            return Ok(Interchange::from_polars_0_41(df)?.to_arrow_55()?);

            #[cfg(feature = "polars-0-42")]
            return Ok(Interchange::from_polars_0_42(df)?.to_arrow_55()?);

            #[cfg(feature = "polars-0-43")]
            return Ok(Interchange::from_polars_0_43(df)?.to_arrow_55()?);

            #[cfg(feature = "polars-0-44")]
            return Ok(Interchange::from_polars_0_44(df)?.to_arrow_55()?);

            #[cfg(feature = "polars-0-45")]
            return Ok(Interchange::from_polars_0_45(df)?.to_arrow_55()?);

            #[cfg(feature = "polars-0-46")]
            return Ok(Interchange::from_polars_0_46(df)?.to_arrow_55()?);

            #[cfg(feature = "polars-0-47")]
            return Ok(Interchange::from_polars_0_47(df)?.to_arrow_55()?);

            #[cfg(feature = "polars-0-48")]
            return Ok(Interchange::from_polars_0_48(df)?.to_arrow_55()?);

            #[cfg(feature = "polars-0-49")]
            return Ok(Interchange::from_polars_0_49(df)?.to_arrow_55()?);

            #[cfg(feature = "polars-0-50")]
            return Ok(Interchange::from_polars_0_50(df)?.to_arrow_55()?);

            // Fallback error if no feature is enabled
            #[cfg(not(any(
                feature = "polars-0-40", feature = "polars-0-41", feature = "polars-0-42",
                feature = "polars-0-43", feature = "polars-0-44", feature = "polars-0-45",
                feature = "polars-0-46", feature = "polars-0-47", feature = "polars-0-48",
                feature = "polars-0-49", feature = "polars-0-50"
            )))]
            Err(PolarsSerdeError::ConversionError {
                message: "No Polars version feature enabled. Please enable one of: polars-0-40, polars-0-41, ..., polars-0-50".to_string()
            })
        }

        /// Convert RecordBatch to DataFrame using the appropriate Polars version
        pub fn arrow_to_dataframe(batches: Vec<RecordBatch>) -> Result<polars::prelude::DataFrame> {
            #[cfg(feature = "polars-0-40")]
            return Ok(Interchange::from_arrow_55(batches)?.to_polars_0_40()?);

            #[cfg(feature = "polars-0-41")]
            return Ok(Interchange::from_arrow_55(batches)?.to_polars_0_41()?);

            #[cfg(feature = "polars-0-42")]
            return Ok(Interchange::from_arrow_55(batches)?.to_polars_0_42()?);

            #[cfg(feature = "polars-0-43")]
            return Ok(Interchange::from_arrow_55(batches)?.to_polars_0_43()?);

            #[cfg(feature = "polars-0-44")]
            return Ok(Interchange::from_arrow_55(batches)?.to_polars_0_44()?);

            #[cfg(feature = "polars-0-45")]
            return Ok(Interchange::from_arrow_55(batches)?.to_polars_0_45()?);

            #[cfg(feature = "polars-0-46")]
            return Ok(Interchange::from_arrow_55(batches)?.to_polars_0_46()?);

            #[cfg(feature = "polars-0-47")]
            return Ok(Interchange::from_arrow_55(batches)?.to_polars_0_47()?);

            #[cfg(feature = "polars-0-48")]
            return Ok(Interchange::from_arrow_55(batches)?.to_polars_0_48()?);

            #[cfg(feature = "polars-0-49")]
            return Ok(Interchange::from_arrow_55(batches)?.to_polars_0_49()?);

            #[cfg(feature = "polars-0-50")]
            return Ok(Interchange::from_arrow_55(batches)?.to_polars_0_50()?);

            // Fallback error if no feature is enabled
            #[cfg(not(any(
                feature = "polars-0-40", feature = "polars-0-41", feature = "polars-0-42",
                feature = "polars-0-43", feature = "polars-0-44", feature = "polars-0-45",
                feature = "polars-0-46", feature = "polars-0-47", feature = "polars-0-48",
                feature = "polars-0-49", feature = "polars-0-50"
            )))]
            Err(PolarsSerdeError::ConversionError {
                message: "No Polars version feature enabled. Please enable one of: polars-0-40, polars-0-41, ..., polars-0-50".to_string()
            })
        }
    };
}

impl_version_conversions!();
