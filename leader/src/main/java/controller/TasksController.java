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

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import zookeeper.ZooController;
import zookeeper.ZooPathTree;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class TasksController extends ZooController {

    protected final static Logger LOG = Logger.getLogger(TasksController.class);

    public TasksController(){
    }

    //TODO
    public void deleteTask(String taskName){

        try {

            zk.delete(ZooPathTree.TASKS.concat("/").concat(taskName), -1);

            //TODO CONTROL EXCEPTIONS
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public void createTask(String taskName, byte[] taskData){

        zk.create(ZooPathTree.TASKS.concat("/").concat(taskName), taskData, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int i, String s, Object o, String s1) {
                switch (KeeperException.Code.get(i)){
                    case CONNECTIONLOSS:
                        createTask(s,s.getBytes());
                        break;

                    case OK:
                        LOG.info( new StringBuilder().append("task ").append(s).append(" moved to tasks"));
                        break;

                    case NODEEXISTS:
                        LOG.info("parent aready registered " + s);
                        break;

                    default:
                        LOG.error("something went wrong: " + KeeperException.create(KeeperException.Code.get(i), s));
                }
            }
        }, this);
    }
}