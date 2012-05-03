## 0.19.1

In elasticsearch.yml:

```yaml
sonian.elasticsearch.equilibrium:
  enabled: true
  minimumAvailablePercentage: 20.0
  minimumSwapDifferencePercentage: 20.0,
  minimumSwapShardRelativeDifferencePercentage: 50.0
```

This plugin currently keeps ES from allocating shards to a node with
less than minimumAvailablePercentage percentage available disk space
on the ES data mount points.

### minimumAvailablePercentage
If a node goes beneath this amount of free disk space percentage, no
more shards will be relocated or assigned to this node. Defaults to 20.0%

### minimumSwapDifferencePercentage
If two nodes have a percentage difference of at least this amount in
used disk space, a shard swap will be initiated. Defaults to 20.0%

### minimumSwapShardRelativeDifferencePercentage
Minimum difference in percentage between two shards to consider them
viable for swapping. For example, if set to 50.0%, the smaller shard
must be <= 50% the size of the largest shard candidate for swapping.
Defaults to 50.0%
