Dropwizard Extra
================

*For those not content with the already excellent [Dropwizard](http://github.com/codahale/dropwizard)*

This is a bunch of additional abstractions and utilities that extemd Dropwizard.

To keep the nightmare of transitive dependencies at bay, there are several 
modules:

  * [dropwizard-extra-archetypes](http://github.com/datasift/dropwizard-extra/tree/develop/dropwizard-extra-archetypes) provide a set of Maven Archetypes for bootstrapping new Dropwizard projects quickly and easily.
  * [dropwizard-extra-core](http://github.com/datasift/dropwizard-extra/tree/develop/dropwizard-extra-common)
  contains several simple but useful abstractions with no real external dependencies.
  * [dropwizard-extra-hbase](http://github.com/datasift/dropwizard-extra/tree/develop/dropwizard-extra-hbase)
  integrates [StumbleUpon's asynchbase](http://github.com/stumbleupon/asynchbase) with Dropwizard for
  working with [HBase](http://hbase.apache.org)
  * [dropwizard-extra-scala](http://github.com/datasift/dropwizard-extra/tree/develop/dropwizard-extra-scala) 
  provides a more idiomatic Scala API to the other Dropwizard Extra modules.

This is still a work-in-progress, so please use what you see with caution.

License
-------

This software is licensed under the [Apache License Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

