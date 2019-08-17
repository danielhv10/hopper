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

import controller.AssignTaskController;
import controller.WorkersController;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import zookeeper.ZooController;
import zookeeper.ZooPathTree;

public class APP extends ZooController implements Runnable {

    WorkersController workersController;
    AssignTaskController assignTaskController;

    private final String appName;

    public static APP createAPP(String appName) {
        APP app = new APP(appName);
        app.setAssignTaskController(new AssignTaskController(app));
        app.setWorkersController(new WorkersController(app));
        return app;
    }

    private APP(String appName){

        this.appName = appName;

        //TODO create those znode async.
        try {

            zk.create(ZooPathTree.TASKS.concat("/").concat(appName), "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create(ZooPathTree.STATUS.concat("/").concat(appName), "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void run() {
        //Get the list of workers and subscribe
        workersController.getWorkersFromZookeeper();
        //Get the list of tasks to asign and subscribe:
        assignTaskController.getZookeeperTasksAndSubscribe();

        //TODO control and stop the thread when is needed
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public WorkersController getWorkersController() {
        return workersController;
    }

    public AssignTaskController getAssignTaskController() {
        return assignTaskController;
    }

    public String getAppName() {
        return appName;
    }

    private void setWorkersController(WorkersController workersController) {
        this.workersController = workersController;
    }

    private void setAssignTaskController(AssignTaskController assignTaskController) {
        this.assignTaskController = assignTaskController;
    }
}
