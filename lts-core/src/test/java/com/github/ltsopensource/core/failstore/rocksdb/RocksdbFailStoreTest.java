package com.github.ltsopensource.core.failstore.rocksdb;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.github.ltsopensource.core.cluster.Config;
import com.github.ltsopensource.core.cluster.NodeType;
import com.github.ltsopensource.core.commons.utils.CollectionUtils;
import com.github.ltsopensource.core.constant.Constants;
import com.github.ltsopensource.core.domain.Job;
import com.github.ltsopensource.core.domain.Pair;
import com.github.ltsopensource.core.failstore.FailStore;
import com.github.ltsopensource.core.failstore.FailStoreException;
import com.github.ltsopensource.core.json.JSON;

/**
 * Robert HG (254963746@qq.com) on 5/27/15.
 */
public class RocksdbFailStoreTest {

	static FailStore failStore;

    private String key = "23412x";

    @Before
    public void setup() throws FailStoreException {
        Config config = new Config();
        config.setIdentity("testIdentity");
        config.setDataPath(Constants.USER_HOME);
        config.setNodeGroup("test");
        config.setNodeType(NodeType.JOB_CLIENT);
        
		if (null == failStore) {
			synchronized (RocksdbFailStoreTest.class) {
				if (null == failStore) {
			        failStore = new RocksdbFailStoreFactory().getFailStore(config, getFailStorePath(config));
			        failStore.open();
				}
			}
		}
    }

    public String getFailStorePath(Config config) {
        return config.getDataPath() + "/.lts" + "/" + config.getNodeType() + "/" + config.getNodeGroup() + "/failstore/";
    }

    @Test
    public void put() throws FailStoreException {
        Job job = new Job();
        job.setTaskId("2131232");
        for (int i = 0; i < 100; i++) {
            failStore.put(key + "" + i, job);
        }
        System.out.println("这里debug测试多线程");
        //failStore.close();
    }

    @Test
    public void fetchTop() throws FailStoreException {
        List<Pair<String, Job>> pairs = failStore.fetchTop(5, Job.class);
        if (CollectionUtils.isNotEmpty(pairs)) {
            for (Pair<String, Job> pair : pairs) {
                System.out.println(JSON.toJSONString(pair));
            }
        }
    }
}