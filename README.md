# ElasticSearch Equilibrium

## Why?

Depending on how you have ES nodes allocated across the system, and
the way the your data goes across indices, it is possible to end up
with a cluster that is weighted (in terms of disk) towards certain
physical nodes. ES-equilibrium was created to ensure that the disk
limit wasn't accidentally reached when creating new shards, and to
allow the ability to balance disk usage across nodes in the cluster.

A good rule of thumb is: **If you don't know whether you need this,
you probably don't need it.**

## Installation

The elasticsearch-equilibrium plugin can be installed as any other ES
plugin using bin/plugin utility:

```
% bin/plugin -install sonian/elasticsearch-equilibrium/0.19.4
```

In elasticsearch.yml:

```yaml
sonian.elasticsearch.equilibrium:
  enabled: true
  minimumAvailablePercentage: 20.0
  minimumSwapDifferencePercentage: 5.0
  minimumSwapShardRelativeDifferencePercentage: 75.0
```

This plugin currently keeps ES from allocating shards to a node with
less than minimumAvailablePercentage percentage available disk space
on the ES data mount points.

### minimumAvailablePercentage
If a node goes beneath this amount of free disk space percentage, no
more shards will be relocated or assigned to this node. Defaults to 20.0%

### minimumSwapDifferencePercentage
If two nodes have a percentage difference of at least this amount in
used disk space, a shard swap will be initiated. Defaults to 5.0%

### minimumSwapShardRelativeDifferencePercentage
Minimum difference in percentage between two shards to consider them
viable for swapping. For example, if set to 50.0%, the smaller shard
must be <= 50% the size of the largest shard candidate for swapping.
Defaults to 75.0%

## Manual rebalancing

```
% curl -s "http://localhost:9200/_rebalance"
```

Manual rebalancing looks through the cluster to discover the node with
the highest disk usage (as a percentage), and the node with the lowers
disk usage (also as a percentage). It then finds the *largest* shard on
the *largest* node and swaps it with the *smallest* shard on the
*smallest* node.

In this way, disk usage that may be uneven across a cluster can be
evened out.

Note: this should only be done under supervision of someone that knows
what they are doing, and only one rebalance should be run at a time.
Wait for the rebalance to finish before starting another.

## POM

If you need to add elasticsearch-equilibrium to your pom.xml (for some
reason), you can use:

```xml
<dependency>
  <groupId>com.sonian</groupId>
  <artifactId>elasticsearch-equilibrium</artifactId>
  <version>0.19.4</version>
</dependency>
```

## Licence

Copyright 2012 Lee Hinman & Sonian

Released under the Apache License, 2.0. See LICENSE.txt
