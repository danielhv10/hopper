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

package controller;


import com.google.gson.Gson;
import model.ZooWorkerDataModel;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;
import zookeeper.Exception.ConnectionNotExistsException;
import zookeeper.ZooController;
import zookeeper.ZooCuratorConnection;
import zookeeper.ZooPathTree;

import java.util.Optional;

public class ZooWorkerController extends ZooController {

    private final static Logger LOG = Logger.getLogger(ZooWorkerController.class);

    private CuratorFramework curatorClient;
    private NodeCache workerDatacache;
    //TODO set it synchronized
    private ChildData zooWorkerData;

    public ZooWorkerController(String workerId, String appName){

        try {

            this.curatorClient = ZooCuratorConnection.getInstance().getCuratorClientConnection();

            workerDatacache = new NodeCache(curatorClient, ZooPathTree.WORKERS.concat("/")
                                                            .concat(appName).concat("/").concat(workerId));

            workerDatacache.getListenable().addListener(() -> {

                try {
                    if (workerDatacache.getCurrentData() != null) {

                        zooWorkerData = workerDatacache.getCurrentData();
                    }
                } catch (Exception e) {
                    LOG.error(e);
                    LOG.error("Error while processing config file change event. message={}");
                }
            });

            //TODO control the correct start of the cache subscribing to the listener
            workerDatacache.start();

            zooWorkerData = workerDatacache.getCurrentData();
        } catch (ConnectionNotExistsException e) {
            LOG.error(e);
            e.printStackTrace();
        } catch (Exception e) {
            LOG.error(e);
            e.printStackTrace();
        }

    }

    public Optional<ChildData> getCurrentWorkerData(){

        return  Optional.ofNullable(zooWorkerData);
    }
}
