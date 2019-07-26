/*
 * Copyright 2019 Hopper
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cache;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import util.PropertiesLoader;
import zookeeper.ZooPathTree;

import java.util.HashSet;
import java.util.Properties;

public class StatusCache {

    private final static Logger LOG = Logger.getLogger(StatusCache.class);

    private static volatile StatusCache instance = null;

    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    CuratorFramework curatorClient;
    TreeCache treeCache;

    public static synchronized StatusCache getInstance(){

        if(instance == null){
            instance = new StatusCache();
        }

        return instance;
    }


    private StatusCache() {

        String zookeeperHost = null;
        int zookeeperPort = 0;

        HashSet<String> expectedProperties = new HashSet<>();
        expectedProperties.add("zookeeper.host");
        expectedProperties.add("zookeeper.port");

        Properties properties = PropertiesLoader.loadProperties(expectedProperties, '.', LOG);

        zookeeperHost = properties.getProperty("zookeeper.host");
        zookeeperPort = Integer.parseInt(properties.getProperty("zookeeper.port"));
        String zookeeperConnectionString = new StringBuilder(zookeeperHost).append(":").append(zookeeperPort).toString();

        curatorClient = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy);

        curatorClient.getUnhandledErrorListenable().addListener((message, e) -> {
           LOG.error("error=" + message);
            e.printStackTrace();
        });

        curatorClient.getConnectionStateListenable().addListener((c, newState) -> {
            LOG.info("state=" + newState);
        });

        curatorClient.start();

        treeCache = TreeCache.newBuilder(curatorClient, ZooPathTree.STATUS).setCacheData(false).build();

        treeCache.getListenable().addListener((c, event) -> {

            if ( event.getData() != null )
            {
                System.out.println("type=" + event.getType() + " path=" + event.getData().getPath());
            }
            else
            {
                System.out.println("type=" + event.getType());
            }
        });

        try {

            treeCache.start();

        } catch (Exception e) {
            LOG.error(e);
            e.printStackTrace();
        }

    }

    public boolean getChachedTaskStatus(String taskId){

        ChildData childData =  treeCache.getCurrentData(ZooPathTree.STATUS.concat("/").concat(taskId));

        if (childData != null){
            return true;
        }
        else{
            return false;
        }
    }
}
