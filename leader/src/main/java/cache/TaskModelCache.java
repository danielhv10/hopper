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
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.log4j.Logger;
import zookeeper.ZooCuratorConnection;
import zookeeper.ZooPathTree;

import java.util.concurrent.CountDownLatch;


public class TaskModelCache{

    private final static Logger LOG = Logger.getLogger(TaskModelCache.class);


    private CuratorFramework curatorClient;
    private TreeCache treeCache;


    public TaskModelCache(){

        CountDownLatch countdown = new CountDownLatch(1);

        try {

            this.curatorClient = ZooCuratorConnection.getInstance().getCuratorClientConnection();
            treeCache = TreeCache.newBuilder(curatorClient, ZooPathTree.TASK_MODEL).setCacheData(false).build();
            treeCache.start();

        } catch (Exception e) {
            LOG.error(e);
            e.printStackTrace();
        }

        treeCache.getListenable().addListener((c, event) -> {


            if (event.getData() != null) {

                switch (event.getType()){

                    case INITIALIZED:
                        LOG.info("wokersTask cache started");
                        if(countdown.getCount() > 0){
                            countdown.countDown();
                        }

                        break;

                    case NODE_ADDED:

                        String taskPath = event.getData().getPath();
                        String taskName = taskPath.substring(taskPath.lastIndexOf("/")+1);

                        if(taskPath.equals(ZooPathTree.TASK_MODEL) && countdown.getCount() > 0){
                            countdown.countDown();
                        }

                        if(!taskPath.equals(ZooPathTree.TASK_MODEL)){
                            LOG.info("New app added" + taskName);
                            APPController.getInstance().addAp(taskName);
                        }

                        break;

                    default:

                        LOG.error("TaskModel cahche error, unknown received value type=" + event.getType());
                        throw new UnsupportedOperationException("Opeation not suported yet");
                }

            } else {

                LOG.info("No data in the taskModel cache");


            }

        });

        try {

            //Waits until cache is ready
            countdown.await();


        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
