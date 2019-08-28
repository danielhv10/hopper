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
import zookeeper.Exception.ConnectionNotExistsException;
import zookeeper.ZooCuratorConnection;
import zookeeper.ZooPathTree;

import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

//TODO start it
public class StatusCache {

    private final static Logger LOG = Logger.getLogger(StatusCache.class);

    private static volatile StatusCache instance = null;

    CuratorFramework curatorClient;
    TreeCache treeCache;

    public static synchronized StatusCache getInstance(){

        if(instance == null){
            instance = new StatusCache();
        }

        return instance;
    }


    private StatusCache() {
        CountDownLatch countdown = new CountDownLatch(1);

        try {
            this.curatorClient = ZooCuratorConnection.getInstance().getCuratorClientConnection();
            treeCache = TreeCache.newBuilder(curatorClient, ZooPathTree.STATUS).setCacheData(false).build();
            treeCache.start();
        } catch (ConnectionNotExistsException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }


        treeCache.getListenable().addListener((c, event) -> {

            if ( event.getData() != null ) {

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
