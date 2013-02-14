Dropwizard Extra
================

*For those not content with the already excellent [Dropwizard](http://github.com/codahale/dropwizard)*

This is a bunch of additional abstractions and utilities that extend Dropwizard.

To keep the nightmare of transitive dependencies at bay, there are several 
modules:

  * [dropwizard-extra-core](http://github.com/datasift/dropwizard-extra/tree/develop/dropwizard-extra-common)
  contains several simple but useful abstractions with no real external dependencies.
  * [dropwizard-extra-curator](http://github.com/datasift/dropwizard-extra/tree/develop/dropwizard-extra-curator)
  integrates [NetFlix's Curator](http://github.com/netflix/curator) high-level [ZooKeeper](http://zookeeper.apache.org)
  client with Dropwizard for working with ZooKeeper directly.
  * [dropwizard-extra-hbase](http://github.com/datasift/dropwizard-extra/tree/develop/dropwizard-extra-hbase)
  integrates [StumbleUpon's asynchbase](http://github.com/stumbleupon/asynchbase) with Dropwizard for
  working with [HBase](http://hbase.apache.org)
  * [dropwizard-extra-kafka](http://github.com/datasift/dropwizard-extra/tree/develop/dropwizard-extra-kafka) for 
  working with [Apache Kafka](http://incubator.apache.org/kafka).
  * [dropwizard-extra-scala](http://github.com/datasift/dropwizard-extra/tree/develop/dropwizard-extra-scala) provides 
  a more idiomatic Scala API to the other Dropwizard Extra modules.
  * [dropwizard-extra-zookeeper](http://github.com/datasift/dropwizard-extra/tree/develop/dropwizard-extra-zookeeper)
  integrates the low-level [Apache ZooKeeper](http://zookeeper.apache.org/) client in to Dropwizards life-cycle. If 
  you're using ZooKeeper directly in your application, it's strongly recommended that you use the higher-level 
  [dropwizard-extra-curator](http://github.com/datasift/dropwizard-extra/tree/develop/dropwizard-extra-curator) 
  instead.

Full documentation for the latest release is available on the 
[generated Maven Site](http://datasift.github.com/dropwizard-extra/).

Usage
-----

Dropwizard Extra is published to [Maven Central](http://search.maven.org/#search|ga|1|g%3Acom.datasift.dropwizard), 
so just add the module(s) you wish to use to your `pom.xml`:

```xml
<dependencies>
    <dependency>
        <groupId>com.datasift.dropwizard</groupId>
        <artifactId>dropwizard-extra-core</artifactId>
        <version>0.6.2-1</version>
    </dependency>
</dependencies>
```

Or whatever you need to do to make SBT/Gradle/Ivy/Buildr/etc. happy.

Versioning
----------

Dropwizard Extra is versioned in lock-step with upstream Dropwizard.

All Dropwizard Extra modules have a transitive dependency on the version of Dropwizard they're built against. The 
versioning scheme for Dropwizard Extra is as follows:

    ${dropwizard.version}.{dw-extra.release.number}

The "release number" signifies the differences between two builds of Dropwizard Extra that are built against the same 
upstream version of Dropwizard.

The practical consequence of this is that an upgrade of Dropwizard Extra will often require an upgrade of Dropwizard 
itself, however, this is always clearly indicated by the version number of Dropwizard Extra itself.

License
-------

This software is licensed under the [Apache License Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

