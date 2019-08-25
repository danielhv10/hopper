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

import main.Worker;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import util.Tuple;
import zookeeper.TaskStatus;
import zookeeper.ZooController;
import zookeeper.ZooPathTree;


import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type.NODE_ADDED;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class WorkerTasksController extends ZooController implements TasksExecutorManager.TasksListener {

    private final static Logger LOG = Logger.getLogger(WorkerTasksController.class);

    private final Worker worker;

    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    CuratorFramework curatorClient;
    TreeCache treeCache;

    private TasksExecutorManager tem;

    @Override
    public void onCompleted(Tuple<String, Tuple<String, Boolean>> result) {

        /*Aquí llega si se COMPLETÓ o se FALLÓ la tarea y la propia TAREA como Object, a la que se le puede hacer Downcasting
        para recuperar la original, es responsabilidad del TaskExecutor devolver bien en 'boolean doTask(T task)'*/
        LOG.info(String.format("%s || %s %s", result.key, result.value.key, result.value.value));
    }


    /////////////////////////////////////
    public WorkerTasksController(Worker worker){
        //TODO SOLVE bad pattern in the object creation
        tem = TasksExecutorManager.getInstance(this);
        tem.setThreads(worker.getMaxAmountOfTasks());
        tem.addTasksListener(this);
        this.worker = worker;

        String zookeeperConnectionString = new StringBuilder(worker.getZookeeperHost()).append(":").append(worker.getZookeeperPort()).toString();

        curatorClient = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy);

        curatorClient.getUnhandledErrorListenable().addListener((message, e) -> {

            LOG.error("error=" + message);
            e.printStackTrace();
        });

        curatorClient.getConnectionStateListenable().addListener((c, newState) -> {

            LOG.info("state=" + newState);
        });

        curatorClient.start();

        try {

            treeCache = TreeCache.newBuilder(curatorClient, ZooPathTree.ASSIGN.concat("/")
                    .concat(worker.getAppName()).concat(ZooPathTree.ASSIGN_WORKER).concat(worker.SERVER_ID))
                    .setCacheData(false).build();

            treeCache.start();
            Thread.sleep(1000);

        } catch (Exception e) {

            LOG.error(e);
            e.printStackTrace();
        }

        treeCache.getListenable().addListener((c, event) -> {

            switch (event.getType()){

                case NODE_ADDED:
                    LOG.info("type=" + event.getType() + " path=" + event.getData().getPath());
                    if(!new String(event.getData().getData()).equals("Idle")) {
                        tem.submit(event.getData().getData(), worker.getExecutorModel(), worker.getTaskModel());
                    }
                    break;

                case NODE_REMOVED:
                    LOG.info("Asigned node deleted");
                    break;

                default:
                    LOG.error("operation not supported" + event.getType() );
                    throw new UnsupportedOperationException("Opeation not suported yet");
            }
        });

        //TODO if a task is asigned to this worker in am empty space between this search and the cache the task can dead in vacuum.
        startAssignedTasks();
    }


    public void addTaskStatus(String task){

        LOG.info("Addindg new task to to status");

        zk.create(ZooPathTree.STATUS.concat("/").concat(task),
                "".getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, new AsyncCallback.StringCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, String name) {

                        switch (KeeperException.Code.get(rc)) {


                            case CONNECTIONLOSS:

                                addTaskStatus(path);

                                break;

                            case OK:

                                LOG.info("task was created correctly".concat(": ").concat(path));

                                break;

                            default:
                                LOG.error("Something went wrong" +  KeeperException.create(KeeperException.Code.get(rc), path));
                        }
                    }
                }, null);

    }

    public Optional<Map<String, ChildData>> getWorkerTasks(){

        LOG.info("worker-".concat(worker.SERVER_ID).concat(" getting tasks"));

        Map<String, ChildData> workerAssignedTasks = treeCache.getCurrentChildren(ZooPathTree.ASSIGN.concat("/")
                .concat(worker.getAppName()).concat(ZooPathTree.ASSIGN_WORKER).concat(worker.SERVER_ID));

        return Optional.ofNullable(workerAssignedTasks);
    }

    /**
     * When worker finish his task it is removed from assignment
     * @param task
     */
    public void endTask(String task) throws KeeperException, InterruptedException {
        //Task Path
        String assignTaskPath = ZooPathTree.ASSIGN_WORKER.concat(worker.SERVER_ID).concat("/").concat(task);
        LOG.info("Ending task ".concat(assignTaskPath));
        zk.delete(assignTaskPath,zk.exists((assignTaskPath), true).getVersion());
    }

    private void startAssignedTasks(){

        Optional listOfTasks = getWorkerTasks();

        if(listOfTasks.isPresent()){
            Map tasks = (Map) listOfTasks.get();

            tasks.forEach((k,v)-> startAssignedTask((String)k));
        }
    }

    private void startAssignedTask(String taskName){

        String taskPath = ZooPathTree.ASSIGN.concat("/")
                .concat(worker.getAppName())
                .concat(ZooPathTree.ASSIGN_WORKER).concat(worker.SERVER_ID).concat("/").concat(taskName);

        zk.getData(taskPath, false, new AsyncCallback.DataCallback() {

            @Override
            public void processResult(int i, String s, Object o, byte[] taskData, Stat stat) {

                switch(KeeperException.Code.get(i)) {

                    case CONNECTIONLOSS:

                        startAssignedTask(taskName);
                        break;

                    case OK:

                        try {

                            tem.submit(taskData, worker.getExecutorModel(), worker.getTaskModel());

                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        } catch (InstantiationException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        break;
                    default:
                        LOG.error(new StringBuilder("TaskDataCallback failed ").append(KeeperException.create(KeeperException.Code.get(i), s)));
                }
            }
        },taskName);
    }

    public void taskDone(String taskName){

        String assingPath = ZooPathTree.ASSIGN.concat("/")
                .concat(worker.getAppName())
                .concat(ZooPathTree.ASSIGN_WORKER).concat(worker.SERVER_ID).concat("/").concat(taskName);


        String statusPath = ZooPathTree.STATUS.concat("/").concat(worker.getAppName()).concat("/").concat(taskName);

        System.out.println(assingPath);
        System.out.println(statusPath);
        byte statusDatatoUpdate[] = "{".concat(TaskStatus.KEYNAME.getText()).concat(": ").
                                    concat(TaskStatus.DONE.getText()).concat("}").getBytes();

        try {

            zk.multi(Arrays.asList(Op.setData(statusPath, statusDatatoUpdate, -1),
                    Op.delete(assingPath, -1)));

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
