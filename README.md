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
  * [dropwizard-extra-db](http://github.com/datasift/dropwizard-extra/tree/develop/dropwizard-extra-db)
  has some additional utilities for working with databases.
  * [dropwizard-extra-hbase](http://github.com/datasift/dropwizard-extra/tree/develop/dropwizard-extra-hbase)
  uses [StumbleUpon's asynchbase](http://github.com/stumbleupon/asynchbase) for
  working with [HBase](http://hbase.apache.org)
  * [dropwizard-extra-kafka](http://github.com/datasift/dropwizard-extra/tree/develop/dropwizard-extra-kafka)
  for consuming Kafka streams in your Dropwizard application.

This is still a work-in-progress, so please use what you see with caution.

