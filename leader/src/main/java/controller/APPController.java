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

import cache.TaskModelCache;
import main.APP;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class APPController {

    private final static Logger LOG = Logger.getLogger(APPController.class);

    List<Thread> apps;

    public static volatile APPController instance;

    public static synchronized APPController getInstance(){
        if(instance == null){
            instance = new APPController();
        }

        return instance;
    }

    private APPController(){

        apps = new ArrayList<Thread>();

    }


    public void addAp(String appName){
        LOG.info("Creating new task ".concat(appName));
        Thread appThread = new Thread(APP.createAPP(appName), appName);
        appThread.start();
        apps.add(appThread);
    }


    private boolean deleteApp(String appName){

        Optional<Thread> optionalThread = apps.stream()
                .filter(x -> appName.equals(x.getName()))
                .findFirst();

        if(optionalThread.isPresent()) {
            Thread foundThread = optionalThread.get();

            foundThread.interrupt();
            return true;
        }
        else{
            return false;
        }
    }

}
