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

import callback.TasksGetChildrenCallback;
import main.Worker;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import util.Tuple;
import zookeeper.ZooController;
import zookeeper.ZooPathTree;


import java.util.HashMap;
import java.util.Map;

import static org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type.NODE_ADDED;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class WorkerTasksController extends ZooController implements TasksExecutorManager.TasksListener {

    private final static Logger LOG = Logger.getLogger(WorkerTasksController.class);

    private final Worker worker;

    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    CuratorFramework curatorClient;
    TreeCache treeCache;

    //NUEVO///////////////////////////////
    //    variables
    private TasksExecutorManager tem;
    //////////////////////////////////////
    //    metodos
    @Override
    public void onCompleted(Tuple<String, Tuple<String, Boolean>> result) {
        /*Aquí llega si se COMPLETÓ o se FALLÓ la tarea y la propia TAREA como Object, a la que se le puede hacer Downcasting
        para recuperar la original, es responsabilidad del TaskExecutor devolver bien en 'boolean doTask(T task)'*/
        LOG.info(String.format("%s || %s %s", result.key, result.value.key, result.value.value));
    }
    /////////////////////////////////////
    public WorkerTasksController(Worker worker){
        tem = TasksExecutorManager.getInstance();
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

        treeCache = TreeCache.newBuilder(curatorClient, ZooPathTree.ASSIGN_WORKER.concat(worker.getAppName() + "/").concat(worker.SERVER_ID)).setCacheData(false).build();

                treeCache.getListenable().addListener((c, event) -> {

                    if ( event.getType() == NODE_ADDED) {

                        LOG.info("type=" + event.getType() + " path=" + event.getData().getPath());

                        //TODO add to pull
                        //NUEVO////////////////////////////
                        //    ejemplo uso
                        if(!new String(event.getData().getData()).equals("Idle")) {
                            tem.submit(event.getData().getData(), worker.getExecutorModel(), worker.getTaskModel());
                        }

                        ///////////////////////////////////

                    }
                    else {

                        LOG.error("operation not supported");
                        throw new UnsupportedOperationException("Opeation not suported yet");
            }

        });

        try {

            treeCache.start();

        } catch (Exception e) {

            LOG.error(e);
            e.printStackTrace();
        }
    }

    /*
    public void createTask(String task, String taskInfo){

        LOG.info("Addindg new task to queue");

        workerTasksCache.addNewTask(task, taskInfo);

        addTaskStatus(task);

        worker.onAssignedTasksUpdated();

    }
    */


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

    public void getWorkerTasks(){

        LOG.info("worker-".concat(worker.SERVER_ID).concat(" getting tasks"));

        Watcher tasksChangeWatcher;


        tasksChangeWatcher = new Watcher() { public void process(WatchedEvent e) {
            if(e.getType() == Watcher.Event.EventType.NodeChildrenChanged) { assert ZooPathTree.ASSIGN.concat(worker.SERVER_ID).equals( e.getPath() );
                getWorkerTasks();
            }
        }};

        LOG.info(ZooPathTree.ASSIGN_WORKER.concat(worker.SERVER_ID));
        zk.getChildren(ZooPathTree.ASSIGN_WORKER.concat(worker.SERVER_ID),  tasksChangeWatcher, new TasksGetChildrenCallback(worker,this), null);
    }

    /*
    public void assignTasks(List<String> tasks){

        tasks.forEach((e) -> {

            if(!this.workerTasksCache.childrenExists(e)){

                zk.getData(ZooPathTree.ASSIGN_WORKER.concat(worker.SERVER_ID), false, new AsyncCallback.DataCallback() {
                    @Override
                    public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {


                        switch(KeeperException.Code.get(i)) {

                            case CONNECTIONLOSS:

                                assignTasks(tasks);
                                break;

                            case OK:

                                LOG.info("Starting task: ".concat((String) o));

                                LOG.info("Task info: ".concat(stat.toString()));
                                LOG.info("Getting task Body");

                                try {

                                    String taskData = new String(zk.getData(s.concat("/").concat((String) o),false, stat));

                                    LOG.info("TaskData: ".concat(taskData));

                                    createTask((String)o, taskData);

                                } catch (KeeperException e) {
                                    LOG.error(e);
                                } catch (InterruptedException e) {
                                    LOG.error(e);

                                }

                                break;

                            default:
                                LOG.error(new StringBuilder("TaskDataCallback failed ").append(KeeperException.create(KeeperException.Code.get(i), s)));

                        }
                    }
                },e);
            }
        });
    }
    */
    /**
     * return current WorkerTaskList
     * @return
     */
    /*
    //TODO change to the new way
    public Map<String, String> getWorkerCachedTaskList(){
        return workerTasksCache.getCachedWorkerList();
    }
    */
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

    /**
     * Zookeeper Tasks tree mirror.
     * it stores current values of /tasks dir
     */
    private class WorkerTasksCache{

        private Map<String, String> workerTasksList;

        public WorkerTasksCache(){

            this.workerTasksList = new HashMap<String, String>();
        }

        public Map<String, String> getWorkerCacheTasksList(){
            return this.workerTasksList;
        }

        public void restartCache(){

            this.workerTasksList = new HashMap<String, String>();
        }


        public synchronized boolean addNewTask(String task, String taskInfo){

            if(!childrenExists(task)){
                this.workerTasksList.put(task, taskInfo);
                return true;
            }else{
                return false;
            }
        }

        /*
        private synchronized List<String>  deletedTasksList(List<String> children){

            List<String> taskCheck = new ArrayList<String>();

            for(String taskToProcess : this.workerTasksList.){

                taskCheck.add(taskToProcess);
            }

            for(String taskToProcess : children){

                if(childrenExists(taskToProcess)){
                    taskCheck.remove(taskToProcess);
                }
            }

            return taskCheck;
        }


        private synchronized List<String> addedTaskList(List<String> children){

            List<String> workerCheck = new ArrayList<String>();


            for(String taskToProcess : children){

                if(!childrenExists(taskToProcess)){
                    workerCheck.add(taskToProcess);
                }
            }

            return workerCheck;

        }


        private synchronized void addNewChildrens(Map<String> tasks){
            tasks.forEach((e) -> this.workerTasksList.add(e));
        }

        private synchronized void deleteNewChildrens(List<String> tasks){
            tasks.forEach((e) -> this.workerTasksList.remove(e));
        }


        public synchronized void  updateCache(List<String> tasksList){
            List<String> taskTemp = null;
            taskTemp = deletedTasksList(tasksList);
            if(taskTemp.size() != 0){
                deleteNewChildrens((taskTemp));
            }

            taskTemp = addedTaskList(tasksList);
            if(taskTemp.size() != 0){
                addNewChildrens(taskTemp);
            }
        }

        */

        private synchronized boolean childrenExists(String children){
            return this.workerTasksList.containsKey(children);
        }

        public Map<String, String> getCachedWorkerList(){
            return this.workerTasksList;
        }
}
}
