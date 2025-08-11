//! Version compatibility layer for different Polars versions

#[cfg(feature = "polars_0_40")]
use polars_crate_0_40 as polars;

#[cfg(feature = "polars_0_41")]
use polars_crate_0_41 as polars;

#[cfg(feature = "polars_0_42")]
use polars_crate_0_42 as polars;

#[cfg(feature = "polars_0_43")]
use polars_crate_0_43 as polars;

#[cfg(feature = "polars_0_44")]
use polars_crate_0_44 as polars;

#[cfg(feature = "polars_0_45")]
use polars_crate_0_45 as polars;

#[cfg(feature = "polars_0_46")]
use polars_crate_0_46 as polars;

#[cfg(feature = "polars_0_47")]
use polars_crate_0_47 as polars;

#[cfg(feature = "polars_0_48")]
use polars_crate_0_48 as polars;

#[cfg(feature = "polars_0_49")]
use polars_crate_0_49 as polars;

#[cfg(feature = "polars_0_50")]
use polars_crate_0_50 as polars;

#[cfg(not(any(
    feature = "polars_0_40",
    feature = "polars_0_41",
    feature = "polars_0_42",
    feature = "polars_0_43",
    feature = "polars_0_44",
    feature = "polars_0_45",
    feature = "polars_0_46",
    feature = "polars_0_47",
    feature = "polars_0_48",
    feature = "polars_0_49",
    feature = "polars_0_50"
)))]
use crate::PolarsSerdeError;
use crate::Result;
use arrow::record_batch::RecordBatch;
#[cfg(any(
    feature = "polars_0_40",
    feature = "polars_0_41",
    feature = "polars_0_42",
    feature = "polars_0_43",
    feature = "polars_0_44",
    feature = "polars_0_45",
    feature = "polars_0_46",
    feature = "polars_0_47",
    feature = "polars_0_48",
    feature = "polars_0_49",
    feature = "polars_0_50"
))]
use df_interchange::Interchange;

/// Macro to generate version-specific conversion functions based on enabled features
macro_rules! impl_version_conversions {
    () => {
        /// Convert DataFrame to RecordBatch using the appropriate Polars version
        pub fn dataframe_to_arrow(mut df: polars::prelude::DataFrame) -> Result<Vec<RecordBatch>> {
            df.as_single_chunk();

            #[cfg(feature = "polars_0_40")]
            return Ok(Interchange::from_polars_0_40(df)?.to_arrow_55()?);

            #[cfg(feature = "polars_0_41")]
            return Ok(Interchange::from_polars_0_41(df)?.to_arrow_55()?);

            #[cfg(feature = "polars_0_42")]
            return Ok(Interchange::from_polars_0_42(df)?.to_arrow_55()?);

            #[cfg(feature = "polars_0_43")]
            return Ok(Interchange::from_polars_0_43(df)?.to_arrow_55()?);

            #[cfg(feature = "polars_0_44")]
            return Ok(Interchange::from_polars_0_44(df)?.to_arrow_55()?);

            #[cfg(feature = "polars_0_45")]
            return Ok(Interchange::from_polars_0_45(df)?.to_arrow_55()?);

            #[cfg(feature = "polars_0_46")]
            return Ok(Interchange::from_polars_0_46(df)?.to_arrow_55()?);

            #[cfg(feature = "polars_0_47")]
            return Ok(Interchange::from_polars_0_47(df)?.to_arrow_55()?);

            #[cfg(feature = "polars_0_48")]
            return Ok(Interchange::from_polars_0_48(df)?.to_arrow_55()?);

            #[cfg(feature = "polars_0_49")]
            return Ok(Interchange::from_polars_0_49(df)?.to_arrow_55()?);

            #[cfg(feature = "polars_0_50")]
            return Ok(Interchange::from_polars_0_50(df)?.to_arrow_55()?);

            // Fallback error if no feature is enabled
            #[cfg(not(any(
                feature = "polars_0_40", feature = "polars_0_41", feature = "polars_0_42",
                feature = "polars_0_43", feature = "polars_0_44", feature = "polars_0_45",
                feature = "polars_0_46", feature = "polars_0_47", feature = "polars_0_48",
                feature = "polars_0_49", feature = "polars_0_50"
            )))]
            Err(PolarsSerdeError::ConversionError {
                message: "No Polars version feature enabled. Please enable one of: polars-0-40, polars-0-41, ..., polars-0-50".to_string()
            })
        }

        /// Convert RecordBatch to DataFrame using the appropriate Polars version
        pub fn arrow_to_dataframe(batches: Vec<RecordBatch>) -> Result<polars::prelude::DataFrame> {
            #[cfg(feature = "polars_0_40")]
            return Ok(Interchange::from_arrow_55(batches)?.to_polars_0_40()?);

            #[cfg(feature = "polars_0_41")]
            return Ok(Interchange::from_arrow_55(batches)?.to_polars_0_41()?);

            #[cfg(feature = "polars_0_42")]
            return Ok(Interchange::from_arrow_55(batches)?.to_polars_0_42()?);

            #[cfg(feature = "polars_0_43")]
            return Ok(Interchange::from_arrow_55(batches)?.to_polars_0_43()?);

            #[cfg(feature = "polars_0_44")]
            return Ok(Interchange::from_arrow_55(batches)?.to_polars_0_44()?);

            #[cfg(feature = "polars_0_45")]
            return Ok(Interchange::from_arrow_55(batches)?.to_polars_0_45()?);

            #[cfg(feature = "polars_0_46")]
            return Ok(Interchange::from_arrow_55(batches)?.to_polars_0_46()?);

            #[cfg(feature = "polars_0_47")]
            return Ok(Interchange::from_arrow_55(batches)?.to_polars_0_47()?);

            #[cfg(feature = "polars_0_48")]
            return Ok(Interchange::from_arrow_55(batches)?.to_polars_0_48()?);

            #[cfg(feature = "polars_0_49")]
            return Ok(Interchange::from_arrow_55(batches)?.to_polars_0_49()?);

            #[cfg(feature = "polars_0_50")]
            return Ok(Interchange::from_arrow_55(batches)?.to_polars_0_50()?);

            // Fallback error if no feature is enabled
            #[cfg(not(any(
                feature = "polars_0_40", feature = "polars_0_41", feature = "polars_0_42",
                feature = "polars_0_43", feature = "polars_0_44", feature = "polars_0_45",
                feature = "polars_0_46", feature = "polars_0_47", feature = "polars_0_48",
                feature = "polars_0_49", feature = "polars_0_50"
            )))]
            Err(PolarsSerdeError::ConversionError {
                message: "No Polars version feature enabled. Please enable one of: polars-0-40, polars-0-41, ..., polars-0-50".to_string()
            })
        }
    };
}

impl_version_conversions!();
