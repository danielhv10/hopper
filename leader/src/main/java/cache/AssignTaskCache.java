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

public class AssignTaskCache {

    protected final static Logger LOG = Logger.getLogger(AssignTaskCache.class);
    private static volatile AssignTaskCache instance = null;

    public List<String> assignningTaskList;

    private AssignTaskCache(){

        this.assignningTaskList = new ArrayList<String>();
    }

    public static synchronized AssignTaskCache getInstance(){

        if(instance == null){
            instance = new AssignTaskCache();
        }

        return instance;
    }


    public void restartCache(){

        this.assignningTaskList = new ArrayList<String>();
    }



    public synchronized boolean newProcessingAssignment(String task){

        if(childrenExists(task)){
            return false;
        }

        else{
            this.assignningTaskList.add(task);
            return true;
        }
    }


    public synchronized boolean doneProcessingAssignment(String task){

        if(childrenExists(task)){
            this.assignningTaskList.remove(task);
            return true;
        }

        else{
            LOG.info("Task: ".concat(task).concat(" doesn't exists"));
            return false;
        }
    }

    public synchronized boolean childrenExists(String children){

        return this.assignningTaskList.contains(children);
    }
}
