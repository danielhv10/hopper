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

import main.APP;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.log4j.Logger;
import zookeeper.Exception.ConnectionNotExistsException;
import zookeeper.ZooCuratorConnection;
import zookeeper.ZooPathTree;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;

public class StatusTasksController {

    private final static Logger LOG = Logger.getLogger(StatusTasksController.class);

    private final APP app;
    private CuratorFramework curatorClient;
    private TreeCache treeCache;

    public StatusTasksController(APP app) {
        this.app = app;

        CountDownLatch countdown = new CountDownLatch(1);

        try {
            curatorClient = ZooCuratorConnection.getInstance().getCuratorClientConnection();
            treeCache = TreeCache.newBuilder(curatorClient, ZooPathTree.STATUS.concat("/").concat(app.getAppName()))
                    .setCacheData(true).build();
        } catch (ConnectionNotExistsException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        treeCache.getListenable().addListener((c, event) -> {
            //TODO refactor taskpath ad taskname variables (repeated in NODE_ADDED and NODE_UPDATED PATH)
            // controlling null values in te callback
            String taskPath;
            String taskName;

                switch (event.getType()){

                    case INITIALIZED:
                        LOG.info("wokersTask cache started");
                            countdown.countDown();

                        break;

                    case NODE_ADDED:

                        taskPath = event.getData().getPath();
                        taskName = taskPath.substring(taskPath.lastIndexOf("/")+1);
                        LOG.info("type=" + event.getType() + " path=" + taskName);
                        break;

                    case NODE_UPDATED:

                        taskPath = event.getData().getPath();
                        taskName = taskPath.substring(taskPath.lastIndexOf("/")+1);

                        LOG.info("APP ".concat(app.getAppName()).concat(" Task ").concat(taskName).concat(" updated"));
                        break;

                    default:

                        LOG.error("deleteTask cache error, unknown received value type=" + event.getType());
                        throw new UnsupportedOperationException("Opeation not suported yet");
                }
        });

        //Waits until cache is ready
        try {
            treeCache.start();
            countdown.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void maangeNodeUpdated(){

    }

    public Optional<ChildData> getTaskStatusData(String taskName){
        ChildData taskStatusData = treeCache.getCurrentData(ZooPathTree.STATUS.concat("/")
                                    .concat(app.getAppName()).concat("/").concat(taskName));

        return  Optional.ofNullable(taskStatusData);
    }
}
