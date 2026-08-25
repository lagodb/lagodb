//! Synthetic PostgreSQL `ctid` carrier for Iceberg physical row identities.

use pg_lakebase_core::api::TRIGGER_ROW_BLOCK_BASE;
use pg_lakebase_core::prelude::ItemPointer;

use super::registry::{ICEBERG_FILE_ID_BITS, IcebergFileId};
use crate::error::{IcebergError, IcebergResult};

/// Compact `(data-file, row-position)` identity carried through PostgreSQL's
/// executor as a synthetic `ctid`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IcebergRowIdentity {
    file_id: IcebergFileId,
    row_position: u32,
}

impl IcebergRowIdentity {
    const fn new(file_id: IcebergFileId, row_position: u32) -> Self {
        Self {
            file_id,
            row_position,
        }
    }

    pub(crate) const fn file_id(self) -> IcebergFileId {
        self.file_id
    }

    pub(crate) const fn row_position(self) -> u32 {
        self.row_position
    }

    pub(crate) fn encode(
        file_id: IcebergFileId,
        position: u64,
    ) -> IcebergResult<ItemPointer> {
        if position > MAX_POSITION {
            return Err(IcebergError::RowIdentityLimitExceeded);
        }
        debug_assert!(u64::from(file_id.raw()) <= FILE_MASK);
        let payload = (u64::from(file_id.raw()) << POSITION_BITS) | position;
        // The validated 17/30-bit payload is at most 2^47 - 1. Dividing that
        // by 65535 yields at most 0x80008000, below both u32::MAX and the
        // 0xC0000000 trigger-row boundary. The remainder plus one is at most
        // u16::MAX.
        let block_number = (payload / OFFSET_BASE) as u32;
        let offset = ((payload % OFFSET_BASE) + 1) as u16;
        debug_assert!(block_number < TRIGGER_ROW_BLOCK_BASE);
        debug_assert_ne!(offset, 0);
        Ok(ItemPointer {
            block_number,
            offset,
        })
    }

    pub(crate) fn decode(tid: &ItemPointer) -> IcebergResult<Self> {
        if tid.offset == 0 || tid.block_number >= TRIGGER_ROW_BLOCK_BASE {
            return Err(IcebergError::InvariantViolated(
                "ctid is not an Iceberg physical row identity",
            ));
        }
        // `block_number < 0xC0000000` and `offset <= u16::MAX`, so this
        // reconstruction is strictly below 2^48 and cannot overflow `u64`.
        let payload =
            u64::from(tid.block_number) * OFFSET_BASE + u64::from(tid.offset - 1);
        if payload >= PAYLOAD_LIMIT {
            return Err(IcebergError::InvariantViolated(
                "ctid is not an Iceberg physical row identity",
            ));
        }
        let raw_file_id = ((payload >> POSITION_BITS) & FILE_MASK) as u32;
        // SAFETY: masking with FILE_MASK establishes the 17-bit ID bound.
        let file_id = unsafe { IcebergFileId::from_valid_raw(raw_file_id) };
        // Masking with the 30-bit POSITION_MASK establishes the u32 bound.
        let row_position = (payload & POSITION_MASK) as u32;
        Ok(Self::new(file_id, row_position))
    }
}

// TODO(synthetic-ctid-capacity): this 17/30-bit split caps one relation at
// 131,072 registered files and each file at 2^30 rows. Target scans may
// register files before quals eliminate all their rows, so redesign the
// identity carrier/registry before workloads can approach either bound. Both
// AM and writable FDW must use this exact protocol.
const POSITION_BITS: u32 = 30;
const MAX_POSITION: u64 = (1u64 << POSITION_BITS) - 1;
const FILE_MASK: u64 = (1u64 << ICEBERG_FILE_ID_BITS) - 1;
const POSITION_MASK: u64 = (1u64 << POSITION_BITS) - 1;
const PAYLOAD_LIMIT: u64 = 1u64 << (ICEBERG_FILE_ID_BITS + POSITION_BITS);
const OFFSET_BASE: u64 = u16::MAX as u64;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synthetic_ctid_round_trips_boundaries() {
        let cases = [
            (0, 0),
            (0, MAX_POSITION),
            (u32::try_from(FILE_MASK).unwrap(), 0),
            (u32::try_from(FILE_MASK).unwrap(), MAX_POSITION),
        ];
        for (file_id, position) in cases {
            let file_id = IcebergFileId::try_from_raw(file_id).unwrap();
            let tid = IcebergRowIdentity::encode(file_id, position).unwrap();
            assert_ne!(tid.offset, 0);
            assert!(tid.block_number < TRIGGER_ROW_BLOCK_BASE);
            let decoded = IcebergRowIdentity::decode(&tid).unwrap();
            assert_eq!(decoded.file_id(), file_id);
            assert_eq!(u64::from(decoded.row_position()), position);
        }
    }

    #[test]
    fn synthetic_ctid_rejects_out_of_range_values() {
        assert!(IcebergFileId::try_from_raw(1 << ICEBERG_FILE_ID_BITS).is_err());
        assert!(
            IcebergRowIdentity::encode(
                IcebergFileId::try_from_raw(0).unwrap(),
                MAX_POSITION + 1,
            )
            .is_err()
        );
        assert!(IcebergRowIdentity::decode(&ItemPointer::default()).is_err());
        assert!(
            IcebergRowIdentity::decode(&ItemPointer {
                block_number: TRIGGER_ROW_BLOCK_BASE,
                offset: 1,
            })
            .is_err()
        );
    }
}
