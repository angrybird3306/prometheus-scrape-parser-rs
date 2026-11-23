// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The prometheus-scrape-parser-rs Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Generated Rust types for the protobuf exposition format:
//! package `io.prometheus.client` (`metrics.proto`), as served in
//! delimited form on scrape endpoints and parsed by
//! [`ProtoBufParser`](crate::ProtoBufParser).

pub mod io {
    pub mod prometheus {
        // Generated code is lint-exempt, per standard practice for
        // checked-in codegen output.
        pub mod client {
            #![allow(clippy::all, rustdoc::all)]

            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/generated/io.prometheus.client.rs"
            ));
        }
    }
}
