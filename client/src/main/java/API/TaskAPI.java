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

package API;

import API.status.AddTaskStatusResponse;
import API.status.DeleteTaskStatusResponse;
import API.status.GetTaskStatusResponse;
import cache.StatusCache;
import controller.APITaskcontroller;
import controller.ZooTaskController;
import model.TaskProperties;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.Optional;
import java.util.Random;

import static spark.Spark.*;

public class TaskAPI implements TaskAPIInterface{

    private final static Logger LOG = Logger.getLogger(TaskAPI.class);

    private  static TaskAPI instance;

    private ZooTaskController zooTaskController;
    private TaskAPIController taskAPIController;

    public static TaskAPI getInstance(){

        if(instance == null){
            instance = new TaskAPI();
        }

        return instance;
    }

    private TaskAPI(){

        this.zooTaskController = new ZooTaskController();
        this.taskAPIController = TaskAPIController.getInstance();
        port(8080);

    }

    public void deployAPI(String appName){

        post("/".concat(appName), (request, response) -> {

            LOG.info("received this body: ".concat(request.body()));

            JSONObject requestParsed = new JSONObject(request.body());

            LOG.info("Adding new task to zookeeper: ".concat(requestParsed.toString()));
            response.type("application/json");


            for(String fieldName : taskAPIController.getRequiredFields(appName)){

                if(!requestParsed.has(fieldName)){
                    response.status(400);
                    return   new JSONObject().put("status", AddTaskStatusResponse.ERROR)
                            .put("message", new StringBuilder("field ").append(fieldName).append("is required"));
                }
            }

            Optional<String> actionStatus = addTask(appName,requestParsed);

            if(actionStatus.isPresent()) {

                response.status(200);
                return new JSONObject().put("taskId", actionStatus.get()).put("status", AddTaskStatusResponse.SUCCESS);
            }
            else {

                response.status(400);
                return new JSONObject().put("status", AddTaskStatusResponse.ERROR).put("message", "Malformed request");
            }
        });

        get("/".concat(appName), (request, response) -> {

            LOG.info("received this body: ".concat(request.body()));

            String requestParsed = request.headers("taskId");

            response.type("application/json");

            GetTaskStatusResponse actionStatus = getTask(requestParsed);

            return new JSONObject().put("status", actionStatus);
        });

        delete("/".concat(appName), (request, response) -> {

            LOG.info("received this body: ".concat(request.body()));

            JSONObject requestParsed = new JSONObject(request.body());

            LOG.info("Adding new task to zookeeper: ".concat(requestParsed.getString("taskId")));
            response.type("application/json");

            DeleteTaskStatusResponse actionStatus = deleteTask(requestParsed.getString("taskId"));

            return new JSONObject().put("status", actionStatus);
        });
    }

    @Override
    public Optional<String> addTask(String appName, JSONObject jsonObject){

        LOG.info("Adding new task to zookeeper: ".concat(jsonObject.toString()));

        String id;

        if( APITaskcontroller.createTaskObject(appName,jsonObject.toMap()).isPresent()){

            id  = Integer.toHexString(new Random().nextInt());

            jsonObject.put(TaskProperties.ID, id);
            jsonObject.put(TaskProperties.APP_NAME, appName);

            zooTaskController.submitNewTask(appName, id, jsonObject);

            return Optional.of(id);

        }else{

            return Optional.empty();
        }
    }

    @Override
    public GetTaskStatusResponse getTask(String taskId) {

       if(StatusCache.getInstance().getChachedTaskStatus(taskId)){

           return GetTaskStatusResponse.SUCCESS;

       } else{

           return GetTaskStatusResponse.DOESNT_EXISTS;
       }
    }

    @Override
    public DeleteTaskStatusResponse deleteTask(String taskId) {

        zooTaskController.submitDeleteTask(taskId);
        return DeleteTaskStatusResponse.SUCCESS;
    }
}