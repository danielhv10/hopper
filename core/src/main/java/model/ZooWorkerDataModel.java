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

package model;

public class ZooWorkerDataModel {

    private String workerId;
    //TODO set this value atomic leader needs it
    private int numAsignedTasks;
    private int maxAmountOfTasks;
    private long headTaskDelay;

    public ZooWorkerDataModel() {
    }

    public ZooWorkerDataModel(String workerId, int numAsignedTasks, int maxAmountOfTasks, long headTaskDelay) {
        this.workerId = workerId;
        this.numAsignedTasks = numAsignedTasks;
        this.maxAmountOfTasks = maxAmountOfTasks;
        this.headTaskDelay = headTaskDelay;
    }

    public int getNumAsignedTasks() {
        return numAsignedTasks;
    }

    public void setNumAsignedTasks(int numAsignedTasks) {
        this.numAsignedTasks = numAsignedTasks;
    }

    public int getMaxAmountOfTasks() {
        return maxAmountOfTasks;
    }

    public void setMaxAmountOfTasks(int maxAmountOfTasks) {
        this.maxAmountOfTasks = maxAmountOfTasks;
    }

    public long getHeadTaskDelay() {
        return headTaskDelay;
    }

    public void setHeadTaskDelay(long headTaskDelay) {
        this.headTaskDelay = headTaskDelay;
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }
}
