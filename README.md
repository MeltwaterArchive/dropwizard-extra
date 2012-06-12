Dropwizard Extra
================

*For those not content with the already excellent [Dropwizard](http://github.com/codahale/dropwizard)*

This is a bunch of additional abstractions and utilities that sit alongside 
Yammer's Dropwizard to allow Scala developers to unleash a little more of the 
power their language affords on Dropwizard projects.

To keep the nightmare of transitive dependencies at bay, there are several 
modules:

  * [dropwizard-extra-common](http://github.com/datasift/dropwizard-extra/tree/develop/dropwizard-extra-common)
  is a few useful abstractions with no real external dependencies.
  * [dropwizard-db](http://github.com/datasift/dropwizard-extra/tree/develop/dropwizard-db)
  has some additional utilities for working with databases.
  * [dropwizard-hbase](http://github.com/datasift/dropwizard-extra/tree/develop/dropwizard-hbase)
  uses [StumbleUpon's asynchbase](http://github.com/stumbleupon/asynchbase) for
  working with [HBase](http://hbase.apache.org)
  * [dropwizard-kafka](http://github.com/datasift/dropwizard-extra/tree/develop/dropwizard-kafka)
  for consuming Kafka streams in your Dropwizard application.

This is still a work-in-progress, so please use what you see with caution.

License
-------

This software is licensed under the [Apache License Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

