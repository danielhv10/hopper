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



import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import model.ZooWorkerDataModel;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.log4j.Logger;
import zookeeper.Exception.ConnectionNotExistsException;
import zookeeper.ZooController;
import zookeeper.ZooCuratorConnection;
import zookeeper.ZooPathTree;

public class ZooWorkerController extends ZooController {

    private final static Logger LOG = Logger.getLogger(ZooWorkerController.class);

    private CuratorFramework curatorClient;
    private NodeCache workerDatacache;

    public ZooWorkerController(String workerId){

        try {

            this.curatorClient = ZooCuratorConnection.getInstance().getCuratorClientConnection();

            workerDatacache = new NodeCache(curatorClient, ZooPathTree.WORKERS.concat("/").concat(workerId));


            //TODO control the correct start of the cache subscribing to the listener
            workerDatacache.start();


        } catch (ConnectionNotExistsException e) {
            LOG.error(e);
            e.printStackTrace();
        } catch (Exception e) {
            LOG.error(e);
            e.printStackTrace();
        }
    }

    public ChildData getCurrentWorkerData(){

        return workerDatacache.getCurrentData();

    }
}
