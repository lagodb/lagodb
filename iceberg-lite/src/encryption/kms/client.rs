// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::Result;
use crate::encryption::SensitiveBytes;

pub struct GeneratedKey {
    key: SensitiveBytes,
    wrapped_key: Vec<u8>,
}

impl GeneratedKey {
    pub fn new(key: SensitiveBytes, wrapped_key: Vec<u8>) -> Self {
        Self { key, wrapped_key }
    }

    pub fn key(&self) -> &SensitiveBytes {
        &self.key
    }

    pub fn wrapped_key(&self) -> &[u8] {
        &self.wrapped_key
    }
}

pub trait KeyManagementClient: Send + Sync + std::fmt::Debug {
    fn wrap_key(&self, key: &[u8], wrapping_key_id: &str) -> Result<Vec<u8>>;

    fn unwrap_key(
        &self,
        wrapped_key: &[u8],
        wrapping_key_id: &str,
    ) -> Result<SensitiveBytes>;

    fn supports_key_generation(&self) -> bool;

    fn generate_key(&self, wrapping_key_id: &str) -> Result<GeneratedKey>;
}
