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

import API.TaskAPIController;
import model.HopperTask;
import model.exceptions.TaskModelException;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;

//TODO manage cath exceptions
//TODO add splicit method params call into getmethod.
//TODO optimize search in get methods
public class APITaskcontroller {


    private final static Logger LOG = Logger.getLogger(APITaskcontroller.class);


    public static Optional<Object> createTaskObject(String appName,Map<String, Object> values)  {

        Optional<Object> taskModelOptional = TaskAPIController.getInstance().createTaskObject(appName);

        if(taskModelOptional.isPresent()){

            for (Map.Entry<String, Object> value: values.entrySet()) {

                try {

                    setTaskfeature(value.getKey(),value.getValue(), taskModelOptional.get());

                } catch (TaskModelException e) {

                    LOG.info("Task scheduling fail: ".concat(e.getMessage()));
                    return Optional.empty();
                }

            }

            return taskModelOptional;

        }else{

            LOG.error("Object creation failed");
            return Optional.empty();
        }
   }


    public static void setTaskfeature(String k, Object v, Object taskModel) throws TaskModelException {

        Method methodList[] =  taskModel.getClass().getDeclaredMethods();

        System.out.println(v.getClass().getName());

        String feature = k.substring(0, 1).toUpperCase().concat(k.substring(1));

        String methodName = "set".concat(feature);


        LOG.info("adding feature ".concat(methodName));
        boolean found = false;

        for(int i = 0; i < methodList.length; ++i){

            if(methodList[i].getName().equals(methodName)){

                found = true;
                try {

                    methodList[i].invoke(taskModel,v);

                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                break;
            }
        }

        if(!found){
            throw new TaskModelException(feature.concat(" not defined in task mmodel"));
        }
    }
}
