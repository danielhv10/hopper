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
import model.exceptions.TaskModelException;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.Random;

import static spark.Spark.*;

public class TaskAPI implements TaskAPIInterface{

    private final static Logger LOG = Logger.getLogger(TaskAPI.class);

    private ZooTaskController zooTaskController;

    public TaskAPI(){

        zooTaskController = new ZooTaskController();

        port(8080);

        post("/", (request, response) -> {

            LOG.info("received this body: ".concat(request.body()));

            JSONObject requestParsed = new JSONObject(request.body());

            LOG.info("Adding new task to zookeeper: ".concat(requestParsed.toString()));
            response.type("application/json");

            String actionStatus = null;

            for(String fieldName : TaskAPIController.getRequiredFields()){

                if(!requestParsed.has(fieldName)){
                    response.status(400);
                    return   new JSONObject().put("status", AddTaskStatusResponse.ERROR)
                            .put("message", new StringBuilder("field ").append(fieldName).append("is required"));
                }
            }

            try{
                actionStatus = addTask(requestParsed);

            }catch (TaskModelException e){
                response.status(400);
                return new JSONObject().put("status", AddTaskStatusResponse.ERROR).put("message", e);
            }

            response.status(200);
            return new JSONObject().put("taskId", actionStatus).put("status", AddTaskStatusResponse.SUCCESS);

        });

        get("/", (request, response) -> {

            LOG.info("received this body: ".concat(request.body()));

            String requestParsed = request.headers("taskId");

            response.type("application/json");

            GetTaskStatusResponse actionStatus = getTask(requestParsed);

            return new JSONObject().put("status", actionStatus);
        });

        delete("/", (request, response) -> {

            LOG.info("received this body: ".concat(request.body()));

            JSONObject requestParsed = new JSONObject(request.body());

            LOG.info("Adding new task to zookeeper: ".concat(requestParsed.getString("taskId")));
            response.type("application/json");

            DeleteTaskStatusResponse actionStatus = deleteTask(requestParsed.getString("taskId"));

            return new JSONObject().put("status", actionStatus);


        });
    }

    @Override
    public String addTask(JSONObject jsonObject) throws TaskModelException {

        //TODO controll the reflexion status.
        LOG.info("Adding new task to zookeeper: ".concat(jsonObject.toString()));

        String id = null;


        new APITaskcontroller().createTaskObject(jsonObject.toMap());

        id = Integer.toHexString(new Random().nextInt());

        //NUEVO///////////////////////

        jsonObject.put(TaskProperties.ID, id);
        jsonObject.put(TaskProperties.APP_NAME, TaskAPIController.APP_NAME);

        /////////////////////////////

        zooTaskController.submitNewTask(id, jsonObject);

        return id;
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