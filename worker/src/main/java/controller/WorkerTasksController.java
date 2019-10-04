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

import afu.org.checkerframework.checker.oigj.qual.O;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import main.Worker;
import model.HopperTask;
import model.ZooWorkerDataModel;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.data.Stat;
import zookeeper.TaskStatus;
import zookeeper.ZooController;
import zookeeper.ZooCuratorConnection;
import zookeeper.ZooPathTree;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class WorkerTasksController extends ZooController implements TasksExecutorManager.TasksListener {

    private final static Logger LOG = Logger.getLogger(WorkerTasksController.class);

    private final Worker worker;

    private CuratorFramework curatorClient;
    private TreeCache treeCache;

    private TasksExecutorManager tem;
    private final Timer timer;

    @Override
    public void onCompleted(String taskId) {
        taskDone(taskId);
        LOG.info("[Finished task] " + taskId);
    }

    public WorkerTasksController(Worker worker) {
        this.worker = worker;
        CountDownLatch countdown = new CountDownLatch(1);
        Gson gson = new Gson();
        //TODO SOLVE bad pattern in the object creation.
        tem = TasksExecutorManager.getInstance();
        tem.addTasksListener(this);//TODO FIXME Dependencia circular entre instancias (Se puede usar un método prepare())

        //Actualización de métrica de delay contra directorio Zookeeper
        timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                LOG.info("Queue size: " + tem.getQueue().size());
                worker.getZooWorkerController().getCurrentWorkerData().ifPresent(childData -> {
                    ZooWorkerDataModel currentWorkerDataModel = gson.fromJson(new String(childData.getData()), ZooWorkerDataModel.class);
                    currentWorkerDataModel.setHeadTaskDelay(tem.getInHeadTaskDelay());
                    LOG.info("Head delay: " + currentWorkerDataModel.getHeadTaskDelay());
                    try {
                        //TODO set async znodeEpoch to control update lock
                        zk.setData(ZooPathTree.WORKERS.concat("/")
                                .concat(worker.getAppName()).concat("/")
                                .concat("worker-".concat(worker.SERVER_ID)),gson.toJson(currentWorkerDataModel).getBytes(),-1);

                    } catch (KeeperException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
            }
        }, 0, 5000);

        try {

            this.curatorClient = ZooCuratorConnection.getInstance().getCuratorClientConnection();


            treeCache = TreeCache.newBuilder(curatorClient, ZooPathTree.ASSIGN.concat("/")
                    .concat(worker.getAppName()).concat(ZooPathTree.ASSIGN_WORKER).concat(worker.SERVER_ID))
                    .setCacheData(false).build();

            treeCache.start();

        } catch (Exception e) {

            LOG.error(e);
            e.printStackTrace();
        }

        treeCache.getListenable().addListener((c, event) -> {

            if(event.getData() != null) {
                switch (event.getType()) {
                    case INITIALIZED:
                        LOG.info("Worker task cache started");
                        countdown.countDown();
                        break;

                    case NODE_ADDED:
                        LOG.info("type=" + event.getType() + " path=" + event.getData().getPath());
                        if (!new String(event.getData().getData()).equals("Idle")) {
                            HopperTask upcastedTask = (HopperTask) new ObjectMapper().readValue(event.getData().getData(), worker.getTaskModel());
                            tem.submitTask(upcastedTask, worker.getExecutorModel());
                        }
                        break;

                    case NODE_REMOVED:
                        String taskPath = event.getData().getPath();
                        String taskId = taskPath.substring(taskPath.lastIndexOf("/") + 1);
                        LOG.info("Deleted task ".concat(taskPath));
                        //TODO Parametrizable en aplicación si interrumpe o no
                        tem.cancelTask(taskId, true);//TODO Controlar si fue efectivamente cancelada o no
                        break;

                    default:
                        LOG.error("Operation not supported" + event.getType());
                        throw new UnsupportedOperationException("Operation not supported yet");
                }

            }else{

                LOG.info("No data in the task cache");
            }

        });

        try {

            //Waits until cache is ready
            countdown.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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
                            HopperTask upcastedTask = (HopperTask) new ObjectMapper().readValue(taskData, worker.getTaskModel());
                            tem.submitTask(upcastedTask, worker.getExecutorModel());
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
        byte statusDatatoUpdate[] = "{".concat(TaskStatus.KEY_STATUS.getText()).concat(": ").
                                    concat(TaskStatus.DONE.getText()).concat(", ")
                .concat(TaskStatus.KEY_WORKERID.getText()).concat(": ").concat(worker.SERVER_ID).concat("}").getBytes();

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
