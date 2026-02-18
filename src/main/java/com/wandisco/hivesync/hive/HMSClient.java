package com.wandisco.hivesync.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

public class HMSClient extends HiveMetaStoreClient {

    public HMSClient(Configuration conf) throws MetaException {
        super(conf);
    }

    public HMSClient createClient() throws MetaException {
        return new HMSClient(conf);
    }
}
