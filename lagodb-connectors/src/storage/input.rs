//! Local-file and object-store input resolution for COPY FROM and foreign scans.

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;

use lagodb_core::storage::foreign::{
    ObjectAccess, ObjectPrefixAccess, StorageManager,
};
use lagodb_storage::StorageError;
use pgrx::check_for_interrupts;
use serde::{Deserialize, Serialize};

use crate::error::ConnectorError;

use super::local_input::LocalInput;
use super::{InputFile, ObjectLocationKind, ResolvedStorageLocation};

const LIST_PAGE_SIZE: u32 = 1_024;

/// Resolved input selection: one storage capability and the stable, sorted
/// membership used for every scan, rescan and parallel worker of a statement.
pub(crate) struct StorageInput {
    members: InputMembers,
    total_bytes: u64,
}

/// Membership together with the capability that opens it.
enum InputMembers {
    Local(Box<[InputMember<PathBuf>]>),
    ObjectExact {
        access: ObjectAccess,
        member: InputMember<Box<str>>,
    },
    ObjectPrefix {
        access: ObjectPrefixAccess,
        members: Box<[InputMember<Box<str>>]>,
    },
}

/// One selected file and the size observed when membership was resolved.
#[derive(Serialize, Deserialize)]
pub(crate) struct InputMember<K> {
    key: K,
    size: u64,
}

impl<K> InputMember<K> {
    pub(super) const fn new(key: K, size: u64) -> Self {
        Self { key, size }
    }
}

/// A leader's resolved membership as transported to parallel workers;
/// storage credentials never enter it.
#[derive(Serialize, Deserialize)]
pub(crate) enum InputInventory {
    /// Unix path bytes, which need not be UTF-8.
    Local(Box<[InputMember<Box<[u8]>>]>),
    /// The exact object's size; its key is the location's.
    ObjectExact {
        size: u64,
    },
    ObjectPrefix(Box<[InputMember<Box<str>>]>),
}

impl StorageInput {
    /// Resolve the declared location once and retain stable prefix membership
    /// for rescans.
    pub(crate) fn resolve(
        location: &ResolvedStorageLocation,
        kind: ObjectLocationKind,
        matches: impl Fn(&str) -> bool,
    ) -> Result<Self, ConnectorError> {
        let location = match location {
            ResolvedStorageLocation::Local(path) => {
                return Ok(Self::new(InputMembers::Local(
                    LocalInput::resolve(path, kind, matches)?.into_boxed_slice(),
                )));
            }
            ResolvedStorageLocation::Object(location) => location,
        };
        let manager = StorageManager::from_pg_gucs()?;
        match kind {
            ObjectLocationKind::Exact => {
                let access = location.acquire_object_access(&manager)?;
                let size = access.head()?.size;
                return Ok(Self::new(InputMembers::ObjectExact {
                    access,
                    member: InputMember::new(location.key().into(), size),
                }));
            }
            ObjectLocationKind::Prefix => {}
        }

        let prefix = location.normalized_prefix();
        let access = location.acquire_prefix_access(&manager, &prefix)?;
        let mut members: Vec<InputMember<Box<str>>> = Vec::new();
        // Prefix scans intentionally materialize and sort one complete LIST.
        // Object stores do not provide a snapshot across independent LISTs;
        // retaining this set gives FDW ReScan stable membership and a stable
        // first object for format-specific schema inference.
        let mut listing = access.listing(LIST_PAGE_SIZE)?;
        loop {
            check_for_interrupts();
            let Some(entries) = listing.next_page()? else {
                break;
            };
            for entry in entries {
                if matches(&entry.key) {
                    members.push(InputMember::new(entry.key.into(), entry.size));
                }
            }
            if listing.is_exhausted() {
                break;
            }
        }
        drop(listing);
        check_for_interrupts();
        members.sort_unstable_by(|left, right| left.key.cmp(&right.key));
        Ok(Self::new(InputMembers::ObjectPrefix {
            access,
            members: members.into_boxed_slice(),
        }))
    }

    /// Rebind membership resolved by a parallel leader without listing or
    /// probing storage again. The leader resolved `inventory` from the same
    /// catalog location under the same snapshot.
    pub(crate) fn reopen(
        location: &ResolvedStorageLocation,
        inventory: InputInventory,
    ) -> Result<Self, ConnectorError> {
        let members = match (location, inventory) {
            (ResolvedStorageLocation::Local(_), InputInventory::Local(members)) => {
                InputMembers::Local(
                    members
                        .into_vec()
                        .into_iter()
                        .map(|member| {
                            let path = PathBuf::from(OsStr::from_bytes(&member.key));
                            InputMember::new(path, member.size)
                        })
                        .collect(),
                )
            }
            (
                ResolvedStorageLocation::Object(location),
                InputInventory::ObjectExact { size },
            ) => {
                let manager = StorageManager::from_pg_gucs()?;
                InputMembers::ObjectExact {
                    access: location.acquire_object_access(&manager)?,
                    member: InputMember::new(location.key().into(), size),
                }
            }
            (
                ResolvedStorageLocation::Object(location),
                InputInventory::ObjectPrefix(members),
            ) => {
                let manager = StorageManager::from_pg_gucs()?;
                InputMembers::ObjectPrefix {
                    access: location.acquire_prefix_access(
                        &manager,
                        &location.normalized_prefix(),
                    )?,
                    members,
                }
            }
            _ => {
                return Err(StorageError::invalid_path(
                    "parallel input inventory does not match the relation location",
                )
                .into());
            }
        };
        Ok(Self::new(members))
    }

    fn new(members: InputMembers) -> Self {
        let mut input = Self {
            members,
            total_bytes: 0,
        };
        input.total_bytes = input
            .member_sizes()
            .fold(0_u64, |total, size| total.saturating_add(size));
        input
    }

    pub(crate) const fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub(crate) fn member_count(&self) -> usize {
        match &self.members {
            InputMembers::Local(members) => members.len(),
            InputMembers::ObjectExact { .. } => 1,
            InputMembers::ObjectPrefix { members, .. } => members.len(),
        }
    }

    /// Member sizes in inventory order.
    pub(crate) fn member_sizes(&self) -> impl Iterator<Item = u64> + '_ {
        let sizes: Box<dyn Iterator<Item = u64> + '_> = match &self.members {
            InputMembers::Local(members) => {
                Box::new(members.iter().map(|member| member.size))
            }
            InputMembers::ObjectExact { member, .. } => {
                Box::new(std::iter::once(member.size))
            }
            InputMembers::ObjectPrefix { members, .. } => {
                Box::new(members.iter().map(|member| member.size))
            }
        };
        sizes
    }

    /// The membership a parallel worker rebinds with [`Self::reopen`].
    pub(crate) fn inventory(&self) -> InputInventory {
        match &self.members {
            InputMembers::Local(members) => InputInventory::Local(
                members
                    .iter()
                    .map(|member| {
                        InputMember::new(
                            member.key.as_os_str().as_bytes().into(),
                            member.size,
                        )
                    })
                    .collect(),
            ),
            InputMembers::ObjectExact { member, .. } => {
                InputInventory::ObjectExact { size: member.size }
            }
            InputMembers::ObjectPrefix { members, .. } => {
                InputInventory::ObjectPrefix(
                    members
                        .iter()
                        .map(|member| {
                            InputMember::new(member.key.clone(), member.size)
                        })
                        .collect(),
                )
            }
        }
    }

    /// Open one member by its inventory position.
    pub(crate) fn open_member(
        &self,
        index: usize,
    ) -> Result<InputFile, ConnectorError> {
        let outside = || {
            StorageError::invalid_path("input member index is outside the inventory")
        };
        let file = match &self.members {
            InputMembers::Local(members) => {
                InputFile::local(&members.get(index).ok_or_else(outside)?.key)
            }
            InputMembers::ObjectExact { access, member } => {
                if index != 0 {
                    return Err(outside().into());
                }
                access
                    .open()
                    .map(|file| InputFile::object(file, &member.key))
            }
            InputMembers::ObjectPrefix { access, members } => {
                let key = &members.get(index).ok_or_else(outside)?.key;
                access
                    .object(key)
                    .and_then(|object| object.open())
                    .map(|file| InputFile::object(file, key))
            }
        };
        file.map_err(ConnectorError::from)
    }

    pub(crate) fn open(self) -> InputFiles {
        InputFiles {
            input: self,
            index: 0,
        }
    }
}

/// Resettable iterator over the files selected by [`StorageInput`].
pub(crate) struct InputFiles {
    input: StorageInput,
    index: usize,
}

impl InputFiles {
    pub(crate) fn reset(&mut self) {
        self.index = 0;
    }
}

impl Iterator for InputFiles {
    type Item = Result<InputFile, ConnectorError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.input.member_count() {
            return None;
        }
        let file = self.input.open_member(self.index);
        self.index += 1;
        Some(file)
    }
}
