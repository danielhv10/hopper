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

package cache;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Zookeeper Tasks tree mirror.
 * it stores current values of /tasks dir
 */
public class TasksCache{

    protected final static Logger LOG = Logger.getLogger(TasksCache.class);

    private static volatile TasksCache instance = null;


    private List<String> TasksList;

    private TasksCache(){

        this.TasksList = new ArrayList<String>();
    }

    public static synchronized TasksCache getInstance(){

        if(instance == null){
            instance = new TasksCache();
        }

        return instance;
    }


    public void restartCache(){

        this.TasksList = new ArrayList<String>();
    }


    private List<String> deletedTasksList(List<String> children){

        List<String> taskCheck = new ArrayList<String>();

        for(String taskToProcess : this.TasksList){

            taskCheck.add(taskToProcess);
        }

        for(String taskToProcess : children){

            if(childrenExists(taskToProcess)){
                taskCheck.remove(taskToProcess);
            }
        }

        return taskCheck;
    }


    private List<String> addedTaskList(List<String> children){

        List<String> workerCheck = new ArrayList<String>();


        for(String taskToProcess : children){

            if(!childrenExists(taskToProcess)){
                workerCheck.add(taskToProcess);
            }
        }

        return workerCheck;
    }

    private void addNewChildrens(List<String> tasks){
        tasks.forEach((e) -> this.TasksList.add(e));
    }

    private void deleteNewChildrens(List<String> tasks){
        tasks.forEach((e) -> this.TasksList.remove(e));
    }

    public void updateCache(List<String> tasksList){
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

    private boolean childrenExists(String children){

        return this.TasksList.contains(children);

    }

    public List<String> getCachedWorkerList(){
        return this.TasksList;
    }
}