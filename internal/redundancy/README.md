# Redundancy package

`internal/redundancy` turns a value into five shards or copies, reconstructs it
from a sufficient subset, and assigns those fragments to nodes deterministically.
It is a pure library: it does not write to storage, call gRPC, or touch Raft.

## Modes

| Mode | Shards | Layout | Reconstruct from |
| --- | --- | --- | --- |
| `rs32` | 5 | indices `0,1,2` data; `3,4` parity; all `ceil(size/3)` bytes | any 3 distinct shards |
| `replication` | 5 | five full copies, indices `0..4`, all data | any 1 checksum-valid copy |

`Mode.Valid()` accepts only the two concrete values. `ParseMode("")` resolves the
empty mode to `replication`, preserving the pre-redundancy default. Callers that
accept a per-request override should parse the string first and then pass the
resolved `Mode` into an engine.

## Shard layout

```text
RS(3,2)                          replication
index 0  data | d0               index 0  full value
index 1  data | d1               index 1  full value
index 2  data | d2               index 2  full value
index 3  parity | p0             index 3  full value
index 4  parity | p1             index 4  full value
```

Indices are global and are copied into placement metadata unchanged. In RS mode
`IsParity` is true only for indices 3 and 4. In replication mode every index is a
data copy, so `IsParity` is false even for indices 3 and 4.

Every `Shard` carries:

- `Index` — global index `0..4`.
- `IsParity` — parity flag derived from the mode, not the index alone.
- `Data` — payload bytes for that shard or copy.
- `Checksum` — lowercase hex SHA-256 of `Data`.
- `TotalSize` — the original value length, repeated for convenience.

## RS(3,2) rules

- `shardSize = max(1, ceil(len(value)/3))`. The minimum of one is required
  because Reed-Solomon cannot encode zero-length shards.
- Value bytes are copied into shards 0-2 in order; the final data shard is
  zero-padded to `shardSize`. `reedsolomon.Encoder.Encode` computes parity shards
  3 and 4.
- Encode always returns exactly five shards with `TotalSize = len(value)`.
- Decode accepts three, four, or five distinct shards. It validates the index
  range, rejects duplicates, verifies every supplied shard checksum, rejects
  inconsistent `TotalSize` values, reconstructs at most two missing shards, and
  verifies the restored parity matrix.
- The three data shards are concatenated and truncated to `totalSize`, so
  padding never appears in the returned value. An empty value therefore round
  trips to an empty slice even though each shard holds one zero byte.
- A corrupted shard fails its checksum before reconstruction. A parity set that
  does not match the data fails the post-reconstruction `Verify`, so a decoded
  payload is never returned on failure.

## Replication rules

- Encode returns five independent full copies of the value, all with the same
  checksum and `TotalSize`.
- Decode returns the first copy whose `TotalSize`, payload length, and checksum
  are valid. Corrupt copies are skipped if a valid copy exists; if none is valid,
  the error lists each mismatch.
- Empty values encode to five zero-length copies and decode to an empty value.

## Deterministic placement

`NewPlacer()` returns the hash placer. `Owners(stripeID, mode, 5, nodes)`:

1. Rejects a shard count other than five.
2. Requires at least five non-empty, distinct node IDs.
3. Sorts a copy of the node list so map or caller order cannot change placement.
4. Computes `offset = FNV-1a(stripeID) % len(nodes)`.
5. Assigns shard `i` to `nodes[(offset+i) % len(nodes)]`.
6. Sets `IsParity` from the mode and index: RS indices 3 and 4 are parity;
   replication indices are all data.

With five nodes this gives each stripe exactly one shard per node, and the hash
rotation spreads first-shard ownership across all nodes. Placement metadata is
therefore deterministic across leaders and restarts.

## Storage math

For a non-empty value of `size` bytes:

```text
RS(3,2) total      = 5 * ceil(size/3)     ~= 1.67 * size   (about 67% extra)
replication total  = 5 * size             =  5.00 * size   (400% extra)
```

The empty value is the documented exception: RS uses five one-byte shards
(`max(1, ...)`), so it costs 5 bytes, while replication costs 0 bytes.

## Failure tolerance

After a healthy write (all five fragments stored) any three live nodes hold
either three RS shards or three replication copies. Three nodes are enough both
for the Raft quorum and for reconstruction, so two node failures are tolerated.
A degraded RS write may store fewer than five shards; those shards do not
necessarily survive two further failures, which makes repair urgent after a
degraded write. Replication degraded writes remain readable through any
surviving copy.

## Integration

The engines are wired into the server write and read paths: the leader encodes,
uploads fragments out of band, and commits metadata only; reads gather and
verify enough fragments before returning a value. Do not add a 3x replication
mode: the cluster has exactly five nodes and every stripe must use all five.
