package com.wandisco.hivesync.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

public class HMSClient extends HiveMetaStoreClient {

    public HMSClient(Configuration conf) throws MetaException {
        super(conf);
    }

    public HMSClient(Configuration conf, HiveMetaHookLoader hookLoader) throws MetaException {
        super(conf, hookLoader);
    }

    public HMSClient(Configuration conf, HiveMetaHookLoader hookLoader, Boolean allowEmbedded) throws MetaException {
        super(conf, hookLoader, allowEmbedded);
    }

    public Configuration getConf() {
        return conf;
    }

    public HMSClient createClient() throws MetaException {
        return new HMSClient(conf);
    }
}
