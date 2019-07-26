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

public class WorkerCacheModel {
    private int maxNumOfTasks;
    private int numAsignedTasks;
    private final String id;

    public  WorkerCacheModel(String workerId){
        this.id = workerId;
    }

    public WorkerCacheModel(String workerId, int maxNumOfTasks) {
        this.maxNumOfTasks = maxNumOfTasks;
            this.numAsignedTasks = numAsignedTasks;
        this.id = workerId;
    }


    public WorkerCacheModel(String workerId, int maxNumOfTasks, int numAsignedTasks) {
        this.maxNumOfTasks = maxNumOfTasks;
        this.numAsignedTasks = numAsignedTasks;
        this.id = workerId;
    }

    public int getMaxNumOfTasks() {
        return maxNumOfTasks;
    }

    public void setMaxNumOfTasks(int maxNumOfTasks) {
        this.maxNumOfTasks = maxNumOfTasks;

    }

    public int getNumAsignedTasks() {
        return numAsignedTasks;
    }

    public String getId(){
        return this.id;
    }


    public void adddoingTask(){
        this.numAsignedTasks++;
    }
}
