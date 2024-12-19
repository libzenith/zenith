use pageserver_api::key::CompactKey;
use utils::bin_ser::{BeSer, DeserializeError, SerializeError};
use utils::lsn::Lsn;

use crate::models::{
    FlushUncommittedRecords, InterpretedWalRecord, InterpretedWalRecords, MetadataRecord,
};

use crate::serialized_batch::{
    ObservedValueMeta, SerializedValueBatch, SerializedValueMeta, ValueMeta,
};

use crate::models::proto;

#[derive(Debug, thiserror::Error)]
pub enum TranscodeError {
    #[error("{0}")]
    BadInput(String),
    #[error("{0}")]
    MetadataRecord(#[from] DeserializeError),
}

impl TryFrom<InterpretedWalRecords> for proto::InterpretedWalRecords {
    type Error = SerializeError;

    fn try_from(value: InterpretedWalRecords) -> Result<Self, Self::Error> {
        let records = value
            .records
            .into_iter()
            .map(proto::InterpretedWalRecord::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(proto::InterpretedWalRecords {
            records,
            next_record_lsn: value.next_record_lsn.map(|l| l.0),
        })
    }
}

impl TryFrom<InterpretedWalRecord> for proto::InterpretedWalRecord {
    type Error = SerializeError;

    fn try_from(value: InterpretedWalRecord) -> Result<Self, Self::Error> {
        let metadata_record = value
            .metadata_record
            .map(|meta_rec| -> Result<Vec<u8>, Self::Error> {
                let mut buf = Vec::new();
                meta_rec.ser_into(&mut buf)?;
                Ok(buf)
            })
            .transpose()?;

        Ok(proto::InterpretedWalRecord {
            metadata_record,
            batch: Some(proto::SerializedValueBatch::from(value.batch)),
            next_record_lsn: value.next_record_lsn.0,
            flush_uncommitted: matches!(value.flush_uncommitted, FlushUncommittedRecords::Yes),
            xid: value.xid,
        })
    }
}

impl From<SerializedValueBatch> for proto::SerializedValueBatch {
    fn from(value: SerializedValueBatch) -> Self {
        proto::SerializedValueBatch {
            raw: value.raw,
            metadata: value
                .metadata
                .into_iter()
                .map(proto::ValueMeta::from)
                .collect(),
            max_lsn: value.max_lsn.0,
            len: value.len as u64,
        }
    }
}

impl From<ValueMeta> for proto::ValueMeta {
    fn from(value: ValueMeta) -> Self {
        match value {
            ValueMeta::Observed(obs) => proto::ValueMeta {
                r#type: proto::ValueMetaType::Observed.into(),
                key: Some(proto::CompactKey::from(obs.key)),
                lsn: obs.lsn.0,
                batch_offset: None,
                len: None,
                will_init: None,
            },
            ValueMeta::Serialized(ser) => proto::ValueMeta {
                r#type: proto::ValueMetaType::Serialized.into(),
                key: Some(proto::CompactKey::from(ser.key)),
                lsn: ser.lsn.0,
                batch_offset: Some(ser.batch_offset),
                len: Some(ser.len as u64),
                will_init: Some(ser.will_init),
            },
        }
    }
}

impl From<CompactKey> for proto::CompactKey {
    fn from(value: CompactKey) -> Self {
        proto::CompactKey {
            high: (value.raw() >> 64) as i64,
            low: value.raw() as i64,
        }
    }
}

impl TryFrom<proto::InterpretedWalRecords> for InterpretedWalRecords {
    type Error = TranscodeError;

    fn try_from(value: proto::InterpretedWalRecords) -> Result<Self, Self::Error> {
        let records = value
            .records
            .into_iter()
            .map(InterpretedWalRecord::try_from)
            .collect::<Result<_, _>>()?;

        Ok(InterpretedWalRecords {
            records,
            next_record_lsn: value.next_record_lsn.map(Lsn::from),
        })
    }
}

impl TryFrom<proto::InterpretedWalRecord> for InterpretedWalRecord {
    type Error = TranscodeError;

    fn try_from(value: proto::InterpretedWalRecord) -> Result<Self, Self::Error> {
        let metadata_record = value
            .metadata_record
            .map(|mrec| -> Result<_, DeserializeError> { MetadataRecord::des(&mrec) })
            .transpose()?;

        let batch = {
            let batch = value.batch.ok_or_else(|| {
                TranscodeError::BadInput("InterpretedWalRecord::batch missing".to_string())
            })?;

            SerializedValueBatch::try_from(batch)?
        };

        Ok(InterpretedWalRecord {
            metadata_record,
            batch,
            next_record_lsn: Lsn(value.next_record_lsn),
            flush_uncommitted: if value.flush_uncommitted {
                FlushUncommittedRecords::Yes
            } else {
                FlushUncommittedRecords::No
            },
            xid: value.xid,
        })
    }
}

impl TryFrom<proto::SerializedValueBatch> for SerializedValueBatch {
    type Error = TranscodeError;

    fn try_from(value: proto::SerializedValueBatch) -> Result<Self, Self::Error> {
        let metadata = value
            .metadata
            .into_iter()
            .map(ValueMeta::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(SerializedValueBatch {
            raw: value.raw,
            metadata,
            max_lsn: Lsn(value.max_lsn),
            len: value.len as usize,
        })
    }
}

impl TryFrom<proto::ValueMeta> for ValueMeta {
    type Error = TranscodeError;

    fn try_from(value: proto::ValueMeta) -> Result<Self, Self::Error> {
        match proto::ValueMetaType::try_from(value.r#type) {
            Ok(proto::ValueMetaType::Serialized) => {
                Ok(ValueMeta::Serialized(SerializedValueMeta {
                    key: value
                        .key
                        .ok_or_else(|| {
                            TranscodeError::BadInput("ValueMeta::key missing".to_string())
                        })?
                        .into(),
                    lsn: Lsn(value.lsn),
                    batch_offset: value.batch_offset.ok_or_else(|| {
                        TranscodeError::BadInput("ValueMeta::batch_offset missing".to_string())
                    })?,
                    len: value.len.ok_or_else(|| {
                        TranscodeError::BadInput("ValueMeta::len missing".to_string())
                    })? as usize,
                    will_init: value.will_init.ok_or_else(|| {
                        TranscodeError::BadInput("ValueMeta::will_init missing".to_string())
                    })?,
                }))
            }
            Ok(proto::ValueMetaType::Observed) => Ok(ValueMeta::Observed(ObservedValueMeta {
                key: value
                    .key
                    .ok_or_else(|| TranscodeError::BadInput("ValueMeta::key missing".to_string()))?
                    .into(),
                lsn: Lsn(value.lsn),
            })),
            Err(_) => Err(TranscodeError::BadInput(format!(
                "Unexpected ValueMeta::type {}",
                value.r#type
            ))),
        }
    }
}

impl From<proto::CompactKey> for CompactKey {
    fn from(value: proto::CompactKey) -> Self {
        (((value.high as i128) << 64) | (value.low as i128)).into()
    }
}
