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

package main;

import controller.TaskController;
import controller.WorkerTasksController;
import model.TaskProperties;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.json.JSONObject;
import zookeeper.ZooPathTree;
import zookeeper.ZooServerConnection;
import zookeeper.ZookeeperEntity;

import java.io.IOException;

public class Worker implements ZookeeperEntity {

    private final static Logger LOG = Logger.getLogger(Worker.class);

    protected WorkersStates status = null;
    protected WorkerTasksController workerTasksController;
    private int maxAmountOfTasks;
    protected final  ZooKeeper zk;
    AsyncCallback.StatCallback  statusUpdateCallback;
    public final String SERVER_ID;

    private Class  taskModel;
    private Class  executorModel;

    private final String zookeeperHost;
    private final int zookeeperPort;

    private String appName;


    //TODO solve exception
    public Worker() throws IOException {
        this.SERVER_ID = ZookeeperEntity.SERVER_ID;
        maxAmountOfTasks = 40;

        this.zookeeperPort = 2181;
        this.zookeeperHost = "localhost";

        zk = ZooServerConnection.getInstance().getZookeeperConnection();

        statusUpdateCallback = new AsyncCallback.StatCallback() {

            public void processResult(int i, String s, Object o, Stat stat) {
                switch (KeeperException.Code.get(i)){

                    case CONNECTIONLOSS:

                        updateStatus(WorkersStates.valueOf((String) o));
                }
            }
        };

        this.workerTasksController = new WorkerTasksController(this);
        this.status = WorkersStates.WAITING;

    }

    public void start(){
        register();

        try {
            Thread.sleep(Long.MAX_VALUE);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    public Worker(String appName, String server, int port, int maxTasks, Class task) throws IOException {
        this.SERVER_ID = ZookeeperEntity.SERVER_ID;
        maxAmountOfTasks = maxTasks;
        this.zookeeperPort = port;
        this.zookeeperHost = server;
        this.appName = appName;

        this.zk = ZooServerConnection.getInstance(server, port).getZookeeperConnection();

        statusUpdateCallback = new AsyncCallback.StatCallback() {

            public void processResult(int i, String s, Object o, Stat stat) {
                switch (KeeperException.Code.get(i)){

                    case CONNECTIONLOSS:

                        updateStatus(WorkersStates.valueOf((String) o));
                }
            }
        };

        this.workerTasksController = new WorkerTasksController(this);
        this.status = WorkersStates.WAITING;
        this.maxAmountOfTasks = maxTasks;
        this.taskModel = task;

        register();
    }

    synchronized private void updateStatus(WorkersStates status){

        if(status == this.status){
            zk.setData(ZooPathTree.WORKERS + "/" + ZookeeperEntity.SERVER_ID, status.toString().getBytes(), -1, statusUpdateCallback, status);
        }
    }

    public void setStatus(WorkersStates status){
        this.status = status;
        updateStatus(status);
    }

    /**
     * Register this server as server into zookeeper.
     *
     * EPHEMERALL nodes
     * It add new entry path /workers/worker-id
     * It add new entry path /assign/worker-id
     *
     * ASYNCHRONOUS response from zookeeper (createWorkerCallback)
     *
     */
    private void register(){

        LOG.info("Registering worker-".concat(ZookeeperEntity.SERVER_ID));

        JSONObject json = new JSONObject();

        json.put("numAsignedTasks",0);
        json.put("maxAmountOfTasks", maxAmountOfTasks);
    /*
        try {

            zk.create(ZooPathTree.WORKERS + "/"  + this.appName, "k".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    */

        //TODO optimize worker scafolding creation
        try {

            zk.create(ZooPathTree.WORKERS + "/".concat(appName), "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            zk.create(ZooPathTree.ASSIGN + "/".concat(appName), "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        createWorkerPath(json);

        createAssigngPath();

        createTaskModel();
    }


    private void createWorkerPath(JSONObject json){

        zk.create(ZooPathTree.WORKERS + "/" + this.appName + "/worker-" + ZookeeperEntity.SERVER_ID, json.toString().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, new AsyncCallback.StringCallback(){

            @Override
            public void processResult(int i, String s, Object o, String s1) {

                switch (KeeperException.Code.get(i)){

                    case CONNECTIONLOSS:
                        register();
                        break;

                    case OK:
                        LOG.info("Registered successfully: " + ZookeeperEntity.SERVER_ID);
                        break;

                    case NODEEXISTS:
                        LOG.warn("already registered:" + ZookeeperEntity.SERVER_ID + ": " + s);
                        break;

                    default:
                        LOG.error("Something went wrong: " + KeeperException.create(KeeperException.Code.get(i), s));
                }

            }
        }, null);

    }


    private void createAssigngPath(){

        zk.create(ZooPathTree.ASSIGN + "/".concat(this.appName) +  "/worker-" + ZookeeperEntity.SERVER_ID, "Idle".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new  AsyncCallback.StringCallback(){

            @Override
            public void processResult(int i, String s, Object o, String s1) {
                switch (KeeperException.Code.get(i)){

                    case CONNECTIONLOSS:
                        register();
                        break;

                    case OK:
                        LOG.info("Assignment created successfully: " + ZookeeperEntity.SERVER_ID);
                        workerTasksController.getWorkerTasks();
                        break;

                    case NODEEXISTS:
                        LOG.warn("already registered:" + ZookeeperEntity.SERVER_ID + ": " + s);
                        break;

                    default:
                        LOG.error("Something went wrong: " + KeeperException.create(KeeperException.Code.get(i), s));
                }
            }
        }, null);
    }

    private void createTaskModel(){

        TaskController taskController = new TaskController();
        String className = null;
        JSONObject json = new JSONObject();

        className = taskModel.getName();

        json.put(TaskProperties.CLASS_NAME, className);
        json.put(TaskProperties.APP_NAME, appName);
        //TODO change this var to new approaching.
        json.put(TaskProperties.SCHEDULING_PLAN, "random");
        json.put(TaskProperties.PROPERTIES, new JSONObject(taskController.getTaskAttributes(taskModel)));

        zk.create(ZooPathTree.TASK_MODEL  + "/".concat(this.appName), json.toString().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new  AsyncCallback.StringCallback(){

            @Override
            public void processResult(int i, String s, Object o, String s1) {
                switch (KeeperException.Code.get(i)){

                    case CONNECTIONLOSS:
                        createTaskModel();
                        break;

                    case OK:
                        LOG.info("Task Created successfully: " + ZookeeperEntity.SERVER_ID);
                        workerTasksController.getWorkerTasks();
                        break;

                    case NODEEXISTS:
                        LOG.warn("Task already exists: APP1");
                        break;

                    default:
                        LOG.error("Something went wrong: " + KeeperException.create(KeeperException.Code.get(i), s));
                }
            }
        }, null);
    }

    public Class getTaskModel() {
        return taskModel;
    }

    public void setTaskModel(Class taskModel) {
        this.taskModel = taskModel;
    }

    public Class getExecutorModel() {
        return executorModel;
    }

    public void setExecutorModel(Class executorModel) {
        this.executorModel = executorModel;
    }

    public String getZookeeperHost() {
        return zookeeperHost;
    }

    public int getZookeeperPort() {
        return zookeeperPort;
    }

    public int getMaxAmountOfTasks() {
        return maxAmountOfTasks;
    }

    public void setMaxAmountOfTasks(int maxAmountOfTasks) {
        this.maxAmountOfTasks = maxAmountOfTasks;
    }

    public String getAppName() {
        return appName;
    }
}


