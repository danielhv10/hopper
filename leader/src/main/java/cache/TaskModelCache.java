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

import controller.APPController;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import util.PropertiesLoader;
import zookeeper.ZooPathTree;

import java.util.HashSet;
import java.util.Properties;


public class TaskModelCache{

    private final static Logger LOG = Logger.getLogger(TaskModelCache.class);


    private CuratorFramework curatorClient;
    private RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    private TreeCache treeCache;

    private String zookeeperHost;
    private int zookeeperPort;

    public TaskModelCache(){

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

        treeCache = TreeCache.newBuilder(curatorClient, ZooPathTree.TASK_MODEL).setCacheData(false).build();

        treeCache.getListenable().addListener((c, event) -> {


            if (event.getData() != null) {

                switch (event.getType()){

                    case NODE_ADDED:
                        String taskPath = event.getData().getPath();
                        String taskName = taskPath.substring(taskPath.lastIndexOf("/")+1);

                        LOG.info("New app added" + taskName);
                        APPController.getInstance().addAp(taskName);
                        break;

                    default:

                        LOG.error("TaskModel cahche error, unknown received value type=" + event.getType());
                        throw new UnsupportedOperationException("Opeation not suported yet");
                }

            } else {

                LOG.error("TaskModel cahche error, no data received");

            }

        });

        try {

            treeCache.start();

        } catch (Exception e) {
            LOG.error(e);
            e.printStackTrace();
        }
    }
}
