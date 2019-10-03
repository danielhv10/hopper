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

import model.ZooWorkerDataModel;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class WorkersCache{

    protected final static Logger LOG = Logger.getLogger(WorkersCache.class);

    private List<ZooWorkerDataModel> workerList;


    public WorkersCache(){

        this.workerList = new ArrayList<ZooWorkerDataModel>();
    }


    public void restartCache(){
        this.workerList = new ArrayList<ZooWorkerDataModel>();
    }


    public List<String> deletedWorkerList(List<String> children){

        List<String> workerCheck = new ArrayList<String>();

        for(ZooWorkerDataModel workerToProcess : this.workerList){

            workerCheck.add(workerToProcess.getWorkerId());
        }

        for(String workerToProcess : children){

            if(childrenExists(workerToProcess)){
                workerCheck.remove(workerToProcess);
            }
        }

        return workerCheck;
    }

    public  List<String> addedWorkerList(List<String> children){

        List<String> workerCheck = new ArrayList<String>();

        for(String workerToProcess : children){

            if(!childrenExists(workerToProcess)){
                workerCheck.add(workerToProcess);
            }
        }

        return workerCheck;
    }


    //TODO manage asynchronously
    public synchronized void addNewChildren(ZooWorkerDataModel worker) {

        this.workerList.add(worker);
    }


    public synchronized void deleteWorker(String worker) throws NoSuchElementException{

        boolean found = false;

        for(int i = 0; i < this.workerList.size(); ++i){
            if(this.workerList.get(i).getWorkerId().equals(worker)){
                this.workerList.remove(i);
                found = true;
            }
        }

        if (!found){
            throw new NoSuchElementException();
        }
    }


    private boolean childrenExists(String children){

        for(ZooWorkerDataModel worker : this.workerList){
            if(worker.getWorkerId().equals(children)){
                return true;
            }
        }
        return  false;
    }

    public List<ZooWorkerDataModel> getCachedWorkerList(){
        return this.workerList;
    }

    public List<String> getCachedWorkerListAsString(){

        List<String> workerIdList = new ArrayList<String>();

        for(ZooWorkerDataModel worker: this.workerList){
            workerIdList.add(worker.getWorkerId());
        }

        return workerIdList;
    }

    public synchronized List<ZooWorkerDataModel> getCachedIddleWorkerLIstAsString(){

        List<ZooWorkerDataModel> workerIddleList = new ArrayList<ZooWorkerDataModel>();

        this.workerList.forEach((e) ->{
            if (e.getNumAsignedTasks() < e.getMaxAmountOfTasks() ){
                workerIddleList.add(e);
            }
        });

        return workerIddleList;
    }
}

