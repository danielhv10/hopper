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

import cache.TasksDeleteCache;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import zookeeper.ZooController;
import zookeeper.ZooPathTree;


public class DeleteTaskController extends ZooController {

    private final static Logger LOG = Logger.getLogger(DeleteTaskController.class);


    TasksDeleteCache deleteCache;


    public DeleteTaskController(){

        deleteCache = TasksDeleteCache.getInstance();
    }

    public void deleteTask(String task){

            LOG.info("Deleting task ".concat(task).concat(" from zookeeper"));

            zk.delete(ZooPathTree.TASKS.concat("/").concat(task), 0, new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx) {
                    switch (KeeperException.Code.get(rc)){

                        case CONNECTIONLOSS:

                            deleteTask(task);
                            break;

                        case OK:
                            LOG.info("Task ".concat(path).concat(" Deleted from Tasks"));

                            taskDeleted(task);
                            break;

                        default:
                            LOG.error("something went wrong when deleting task", KeeperException.create(KeeperException.Code.get(rc), path));
                    }
                }
            }, null);
    }

    public void DeleteAssignedTask(){

    }

    public void taskDeleted(String taskName){

        try {
            LOG.info("Deleting task ".concat(taskName).concat(" from delete"));
            zk.delete(ZooPathTree.TASK_DELETE.concat("/").concat(taskName),0);

        } catch (InterruptedException e) {
            e.printStackTrace();

        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
