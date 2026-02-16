# hive-metastore-sync
Replicate hive metastore from the cluster to another one

## How to build?
* Run maven build:
```
mvn clean package -DskipTests
```

Maven will produce two results:

1. directory ```target/hive-metastore-sync-<version>/```
2. zip archive ```target/hive-metastore-sync-<version>.zip```

## How to test?
* Need installed docker and direct access to internet
* Run the tests:

```
mvn test
```

Test suite starts/restarts hadoop and hive every time you run it. You can cut this time by specifying parameter ```skipStart```:

```
mvn test -DskipStart=true
````

## Running hive-metastore-sync

To run hive-metastore-sync from shell:

```
<install-dir>/bin/hivesync [parameters]
```

## Configuration
Log4j2 configuration is stored in ```<install-dir>/conf/log4j2.xml```
The default configuration file produces log file ```/tmp/hive-metastore-sync.txt```

## Configuring secure Hive sync

To configure secure Hive sync, you must configure cross realm support for Kerberos and Hadoop

#### Kerberos & Hadoop

* Create krbtgt principals for the two realms. For example, if you have two realms called SRC.COM and DST.COM, then you
need to add the following principals: krbtgt/SRC.COM@DST.COM and krbtgt/DST.COM@SRC.COM. Add these two principals at both realms.
Note that passwords should be the same for both realms:
```
kadmin: addprinc krbtgt/SRC.COM@DST.COM
kadmin: addprinc krbtgt/DST.COM@SRC.COM
```
* You would need to add respective auth_to_local mapping for SRC cluster principals into your destination cluster Hadoop configuration core-site.xml file. For example, to add maopping for the hdfs/SRC.COM principal into destination cluster:
```
core-site.xml

<property>
  <name>hadoop.security.auth_to_local</name>
  <value>
RULE:[2:$1@$0](hdfs@SRC.COM)s/.*/hdfs/
RULE:[1:$1@$0](hdfs@SRC.COM)s/.*/hdfs/
DEFAULT
  </value>
</property>
```
* Add destination realm into /etc/krb5.conf on source cluster:
```
/etc/krb5.conf

[realms]
...
  DST.COM = {
    admin_server = admin_server.dst.com
    kdc = kdc_server.dst.com
  }

[domain_realm]
.dst.com = DST.COM
dst.com = DST.COM
```

* Add source realm into /etc/krb5.conf on destination cluster:
```
/etc/krb5.conf

[realms]
...
  SRC.COM = {
    admin_server = admin_server.src.com
    kdc = kdc_server.src.com
  }

[domain_realm]
.src.com = SRC.COM
src.com = SRC.COM
```

#### Check configuration
* How to check that kerberos has been configured correctly:
On the destination cluster check that principal from the source cluster could be mapped correctly:
```
$ hadoop org.apache.hadoop.security.HadoopKerberosName hdfs@SRC.COM
```
On the source cluster run beeline and simple query (hive principal is used there):
```
$ beeline
beeline> !connect jdbc:hive2://hiveserver.dst.com:10000/default;principal=hive/dst.com@DST.COM
beeline> show tables;
```
#### Start sync
* Use ```--dry-run``` to generate log with hive commands:
```
<install-dir>/bin/hivesync --dry-run --src-meta "thrift://hms.src.com:9083" --dst-meta "thrift://hms.dst.com:9083"
```
* Review output log and run the same command again, but without ```--dry-run``` parameter to start syncing.

#### Запуск локальных баз для отладки

    docker compose -f hive1.yaml up -d
    docker compose -f hive2.yaml up -d

    docker exec -it hive1-hiveserver-1 beeline -u 'jdbc:hive2://localhost:10000/'
    docker exec -it hive2-hiveserver-1 beeline -u 'jdbc:hive2://localhost:10000/'
