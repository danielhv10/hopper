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

import cache.AssignTaskCache;
import cache.WorkerCacheModel;
import com.oracle.tools.packager.Log;
import main.APP;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import zookeeper.ZooController;
import zookeeper.ZooPathTree;


import java.util.List;

public class AssignTaskController extends ZooController {

    private final static Logger LOG = Logger.getLogger(AssignTaskController.class);
    private final APP app;
    private final String zooTasksPath;
    private final String zooAssignmentPath;


    public final AssignTaskCache assignTaskCache;


    public AssignTaskController(APP app) {

        this.app = app;
        this.zooAssignmentPath = ZooPathTree.ASSIGN.concat("/").concat(app.getAppName());
        this.zooTasksPath = ZooPathTree.TASKS.concat("/").concat(app.getAppName());
        this.assignTaskCache = new AssignTaskCache();

    }

    public  boolean startAssignment(String task){

        return assignTaskCache.newProcessingAssignment(task);
    }

    public  boolean endAssignment(String task){
       return assignTaskCache.doneProcessingAssignment(task);
    }


    public void getZookeeperTasksAndSubscribe() {

        Watcher tasksChangeWatcher;


        tasksChangeWatcher = new Watcher() { public void process(WatchedEvent e) {
            if(e.getType() == Watcher.Event.EventType.NodeChildrenChanged) { assert zooTasksPath.equals( e.getPath() );
                getZookeeperTasksAndSubscribe();
            }
        }};


        zk.getChildren(zooTasksPath,  tasksChangeWatcher, new AsyncCallback.ChildrenCallback() {
            @Override
            public void processResult(int i, String s, Object o, List<String> children) {
                switch (KeeperException.Code.get(i)){

                    case CONNECTIONLOSS:

                        getZookeeperTasksAndSubscribe();
                        break;

                    case OK:

                        if(children != null){
                            Log.info("new task added in app: ".concat(app.getAppName()));
                            assignTasks(children);
                        }
                        break;

                    default:

                        LOG.error(new StringBuilder().append("getChildren failed ").append(KeeperException.create(KeeperException.Code.get(i), s)));

                }
            }
        }, null);
    }

    public void getZookeeperTasks() {

        zk.getChildren(zooTasksPath,  null, new AsyncCallback.ChildrenCallback() {
            @Override
            public void processResult(int i, String s, Object o, List<String> children) {
                switch (KeeperException.Code.get(i)){

                    case CONNECTIONLOSS:

                        getZookeeperTasksAndSubscribe();
                        break;

                    case OK:

                        if(children != null){

                            assignTasks(children);
                        }
                        break;

                    default:

                        LOG.error(new StringBuilder().append("getChildren failed ").append(KeeperException.create(KeeperException.Code.get(i), s)));

                }
            }
        }, null);
    }


    public  void createAssignment(WorkerCacheModel workerCacheModel, String taskName, byte[] data) {

        String path = zooAssignmentPath.concat("/").concat(workerCacheModel.getId()).concat("/").concat(taskName);


        if (startAssignment(taskName)){

            super.zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, new AsyncCallback.StringCallback() {

                @Override
                public void processResult(int i, String s, Object o, String s1) {
                    TasksController tasksController = new TasksController();

                    LOG.info(path);
                    //Get the taskName without the path.
                    String taskName = null;
                    String[] parts = path.split("/");
                    taskName = parts[parts.length -1];

                    switch(KeeperException.Code.get(i)) {

                        case CONNECTIONLOSS:

                            createAssignment(workerCacheModel, taskName, data);
                            break;

                        case OK:


                            LOG.info("Task assigned correctly: " + taskName);
                            LOG.info(taskName);

                            //Delete task from tasks
                            tasksController.deleteTask(app.getAppName(),taskName);


                            //Delete task from assignment cache
                            endAssignment(taskName);

                            //add using space worker in zookeeper
                            app.getWorkersController().doneAssignmentTask(workerCacheModel);

                            break;

                        case NODEEXISTS:
                            LOG.warn("Task already assigned");

                            break;

                        default:
                            LOG.error("Error when trying to assign task.",
                                    KeeperException.create(KeeperException.Code.get(i), path));
                    }
                }
            }, data);

        }
        else{
            LOG.info("task: ".concat(data.toString()).concat("Is already in assignment"));
        }
    }


    public void assignTasks(List<String> tasks){

        tasks.forEach((e) ->{

            if(!this.assignTaskCache.childrenExists(e)){

                zk.getData(zooTasksPath.concat("/").concat(e), false, new AsyncCallback.DataCallback() {
                    @Override
                    public void processResult(int i, String path, Object o, byte[] data, Stat stat) {
                        switch(KeeperException.Code.get(i)) {

                            case CONNECTIONLOSS:

                                zk.getData(zooTasksPath.concat("/").concat(e), false, this,e);
                                break;

                            case OK:

                                    WorkerCacheModel designatedWorker = app.getWorkersController().bookIdleWorker();

                                    createAssignment(designatedWorker, (String) o, data);

                                break;

                            default:
                                LOG.error(new StringBuilder("TaskDataCallback failed ").append(KeeperException.create(KeeperException.Code.get(i), path)));
                        }
                    }
                }, e);
            }

        });
    }


    //TODO asynchronous delete control
    public void deleteAssignment(String workerName, String taskName){

        String assignTask = zooAssignmentPath.concat("/").concat(workerName).concat("/").concat(taskName);
        LOG.info("deleting ".concat(assignTask));
        try {

            zk.delete(assignTask, -1);

            //TODO CONTROL EXCEPTIONS
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}

