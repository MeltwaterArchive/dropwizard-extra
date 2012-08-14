Dropwizard Extra
================

*For those not content with the already excellent [Dropwizard](http://github.com/codahale/dropwizard)*

This is a bunch of additional abstractions and utilities that extend Dropwizard.

To keep the nightmare of transitive dependencies at bay, there are several 
modules:

  * [dropwizard-extra-core](http://github.com/datasift/dropwizard-extra/tree/develop/dropwizard-extra-common)
  contains several simple but useful abstractions with no real external dependencies.
  * [dropwizard-extra-hbase](http://github.com/datasift/dropwizard-extra/tree/develop/dropwizard-extra-hbase)
  integrates [StumbleUpon's asynchbase](http://github.com/stumbleupon/asynchbase) with Dropwizard for
  working with [HBase](http://hbase.apache.org)
  * [dropwizard-extra-kafka](http://github.com/datasift/dropwizard-extra/tree/develop/dropwizard-extra-kafka) for working with [Apache Kafka](http://incubator.apache.org/kafka).
  * [dropwizard-extra-scala](http://github.com/datasift/dropwizard-extra/tree/develop/dropwizard-extra-scala) 
  provides a more idiomatic Scala API to the other Dropwizard Extra modules.

Full documentation for the latest release is available on the [generated Maven Site](http://datasift.github.com/dropwizard-extra/).

License
-------

This software is licensed under the [Apache License Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

