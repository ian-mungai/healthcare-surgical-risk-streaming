# Hand-built boundary cases

This folder is the only place for a case that a person builds by hand to reach one edge that no published recording reaches, for example a reading on a threshold or a gap of one second. It holds nothing now.

- A case here is never mixed with published data. It is never listed in `tests/fixtures/vitaldb/` and never counted as a candidate for the fixed sample.
- Each file is named for the edge it reaches, and the test that uses it says why a published recording cannot reach that edge.
- The project keeps published and invented data apart: small fixed samples for tests come from the published recordings, which `tests/fixtures/vitaldb/` pins by key and checksum.
