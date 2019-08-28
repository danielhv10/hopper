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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.log4j.Logger;
import zookeeper.Exception.ConnectionNotExistsException;
import zookeeper.ZooCuratorConnection;
import zookeeper.ZooPathTree;


import java.util.concurrent.CountDownLatch;


public class TasksDeleteCache {

    private final static Logger LOG = Logger.getLogger(TasksDeleteCache.class);

    private final String appName;
    private CuratorFramework curatorClient;
    private TreeCache treeCache;


    public TasksDeleteCache(String appName) {

        CountDownLatch countdown = new CountDownLatch(1);

        this.appName = appName;

        try {
            curatorClient = ZooCuratorConnection.getInstance().getCuratorClientConnection();
            treeCache = TreeCache.newBuilder(curatorClient, ZooPathTree.TASK_DELETE.concat("").concat(appName))
                    .setCacheData(false).build();

            treeCache.start();
        } catch (ConnectionNotExistsException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }


        treeCache.getListenable().addListener((c, event) -> {

            if(event.getData() != null){

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

                        if(taskPath.equals(ZooPathTree.TASK_DELETE.concat("").concat(appName)) && countdown.getCount() > 0){
                            countdown.countDown();
                        }

                        LOG.info("type=" + event.getType() + " path=" + taskName);
                        //TODO add to the new multiapp approaching
                        // new DeleteTaskController().deleteTask(taskName);

                    default:

                        LOG.error("deleteTask cache error, unknown received value type=" + event.getType());
                        throw new UnsupportedOperationException("Opeation not suported yet");
                }
            }else{

                throw new UnknownError("Path doesn't exists");
            }

        });

        //Waits until cache is ready
        try {
            countdown.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public boolean getChacheddeleteTask(String taskId){

        ChildData childData =  treeCache.getCurrentData(ZooPathTree.TASK_DELETE.concat("/").concat(taskId));

        if (childData != null){
            return true;
        }
        else{
            return false;
        }
    }

}
