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


import main.APP;
import model.ZooWorkerDataModel;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.json.JSONObject;
import zookeeper.Exception.ConnectionNotExistsException;
import zookeeper.TaskStatus;
import zookeeper.ZooController;
import zookeeper.ZooCuratorConnection;
import zookeeper.ZooPathTree;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

public class DeleteTaskController extends ZooController {

    private final static Logger LOG = Logger.getLogger(DeleteTaskController.class);

    private final APP app;
    private CuratorFramework curatorClient;
    private TreeCache treeCache;

    public DeleteTaskController(APP app){
        this.app = app;
        CountDownLatch countdown = new CountDownLatch(1);

        try {
            curatorClient = ZooCuratorConnection.getInstance().getCuratorClientConnection();
            treeCache = TreeCache.newBuilder(curatorClient, ZooPathTree.TASK_DELETE.concat("/").concat(app.getAppName()))
                    .setCacheData(false).build();

        } catch (ConnectionNotExistsException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        treeCache.getListenable().addListener((c, event) -> {
            String taskPath;
            String taskId;

                switch (event.getType()){

                    case INITIALIZED:
                        LOG.info("wokersTask cache started");
                        countdown.countDown();

                        break;

                    case NODE_ADDED:
                        taskPath = event.getData().getPath();
                        taskId = taskPath.substring(taskPath.lastIndexOf("/")+1);

                        LOG.info("type=" + event.getType() + " path=" + taskId);

                        if(!taskPath.equals(ZooPathTree.TASK_DELETE.concat("/").concat(app.getAppName()))){
                             deleteTask(taskId,5);
                        }

                        break;
                    case NODE_REMOVED:

                        taskPath = event.getData().getPath();
                        taskId = taskPath.substring(taskPath.lastIndexOf("/")+1);
                        LOG.info("APP ".concat(app.getAppName()).concat(" Deleted task: ").concat(taskId));
                        
                        break;
                    default:

                        LOG.error("deleteTask cache error, unknown received value type=" + event.getType());
                        throw new UnsupportedOperationException("Opeation not suported yet");
                 }
        });


        //Start the cache and Waits until cache is ready
        try {
            treeCache.start();
            countdown.await();
            LOG.info("Delete Task cache inited");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void deleteTask(String taskId, int watchdog) throws KeeperException.NoNodeException {
        //Max number of delete task try's.
        if(watchdog == 0){
            return;
        }

      Optional<ChildData> statusChildData =  app.getStatusTasksController().getTaskStatusData(taskId);

      if(statusChildData.isPresent()){

          JSONObject json = new JSONObject(new String(statusChildData.get().getData()));

          switch (TaskStatus.valueOf(json.getString(TaskStatus.KEY_STATUS.getText()).toUpperCase())){

              case ASIGNED:

                  String workerId = json.getString(TaskStatus.KEY_WORKERID.getText());

                  String assingPath = ZooPathTree.ASSIGN.concat("/").concat(app.getAppName()).concat("/")
                          .concat(workerId).concat("/").concat(taskId);
                  String deletePath = ZooPathTree.TASK_DELETE.concat("/").
                          concat(app.getAppName()).concat("/").concat(taskId);
                  String  statusPath = ZooPathTree.STATUS.concat("/").concat(app.getAppName()).concat("/").concat(taskId);

                  try {

                      zk.multi(Arrays.asList(
                              Op.delete(assingPath, -1),
                              Op.delete(statusPath, statusChildData.get().getStat().getVersion()),
                              Op.delete(deletePath, -1)));

                      Optional<ZooWorkerDataModel> workerDataOptional = app.getWorkersController().getWorkerDatabyID(workerId);
                      if(workerDataOptional.isPresent()){
                          //TODO do it atomicly
                          workerDataOptional.get().setNumAsignedTasks(workerDataOptional.get().getNumAsignedTasks() -1);
                          app.getWorkersController().doneAssignmentTask(workerDataOptional.get());
                          app.getAssignTaskController().getZookeeperTasks(); //reasigntask if there are new queued.

                      }else{
                          LOG.info("worker ".concat(workerId).concat(" doesn't found"));
                      }

                  } catch (InterruptedException e) {
                      e.printStackTrace();
                  } catch (KeeperException e) {
                      LOG.info("APP: ".concat(app.getAppName()).concat(" task: ")
                              .concat(taskId).concat(" Dele has failed, task status Has changed, tryying again"));
                      deleteTask(taskId, --watchdog);
                  }

                  break;


              case PENDING:

                  try {
                      zk.multi(Arrays.asList(
                              Op.delete(ZooPathTree.TASKS.concat("/").concat(app.getAppName()).concat("/").concat(taskId),-1),
                              Op.delete(ZooPathTree.TASK_DELETE.concat("/").concat(app.getAppName()).concat("/").concat(taskId),-1)
                      ));
                  } catch (InterruptedException e) {
                      e.printStackTrace();
                  } catch (KeeperException e) {
                      LOG.info("APP: ".concat(app.getAppName()).concat(" task: ")
                              .concat(taskId).concat(" Dele has failed, task status Has changed, tryying again"));
                      deleteTask(taskId, --watchdog);
                  }

                  break;

              default:

                  throw new UnsupportedOperationException("Opeation not suported yet");
          }


      }else{
          LOG.info("task child dont found in app: ".concat(app.getAppName()).concat(" task: ".concat(taskId)));
          throw new KeeperException.NoNodeException();
      }

    }
}
