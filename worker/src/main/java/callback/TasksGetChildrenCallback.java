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

package callback;

import controller.WorkerTasksController;
import main.Worker;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;

import java.util.List;

public class TasksGetChildrenCallback implements AsyncCallback.ChildrenCallback {

    private final static Logger LOG = Logger.getLogger(TasksGetChildrenCallback.class);

    private final Worker worker;
    private final WorkerTasksController workerTasksController;


    public TasksGetChildrenCallback(Worker worker, WorkerTasksController workerTasksController){
        this.worker = worker;
        this.workerTasksController = workerTasksController;
    }

    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children) {
    /*
        LOG.info("New tasks" + (String) ctx);

        switch (KeeperException.Code.get(rc)){

            case CONNECTIONLOSS:

              //  worker.getTasks();
                break;

            case OK:
                if(children != null){
                    LOG.info("AssingTaskCallback call");

                    workerTasksController.assignTasks(children);
                }
                break;

            case NONODE:
                LOG.info("problem fonds" + (String) ctx);
                break;
            default:
                LOG.error("Status not controlled".concat( KeeperException.Code.get(rc).toString()));
                break;
        }
    */
    }
}
