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

import com.google.common.collect.Multimap;
import model.Required;
import model.TaskProperties;
import model.HopperTask;
import net.bytebuddy.ByteBuddy;

import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.*;
//TODO change the static approaching

public class TaskAPIController {

    private final static Logger LOG = Logger.getLogger(TaskAPIController.class);

    private  static TaskAPIController INSTANCE;


    public static synchronized TaskAPIController getInstance(){
        if(INSTANCE == null){
            INSTANCE = new TaskAPIController();
        }

        return INSTANCE;
    }

    private List<String> appNames;
    public Multimap<String, String> requiredFieldsMap;
    private Map<String,DynamicType.Builder<?>> taskModelMap;


    private TaskAPIController(){

        this.taskModelMap = new HashMap<>();
        this.appNames = new ArrayList<>();
    }

    public  List<String> getRequiredFields(String appName){


        return  new ArrayList<>(requiredFieldsMap.get(appName));
    }


    public  void updateAPI(JSONObject taskJSON){

        LOG.info("New task model asigned");
        String appName = taskJSON.getString(TaskProperties.APP_NAME);

        this.appNames.add(appName);

        //TODO mach this two collections better

        Map<String, Object> props = new HashMap<String, Object>();


        taskJSON.getJSONObject(TaskProperties.PROPERTIES).toMap().forEach((k,v) -> {

                props.put(k, v);
            }
        );

        props.forEach((k,v) -> LOG.info(k + ", " + v));

        this.taskModelMap.put(appName, createBeanClass(taskJSON.getString(TaskProperties.CLASS_NAME), props));

        updateRequiredProperties(appName);

        TaskAPI.getInstance().deployAPI(appName);
    }

    public  Optional<Object> createTaskObject(String taskName){

        try {

            return Optional.of(taskModelMap.get(taskName).make()
                    .load(ClassLoader.getSystemClassLoader(), ClassLoadingStrategy.Default.WRAPPER)
                    .getLoaded()
                    .newInstance());

        } catch (InstantiationException e) {

            e.printStackTrace();
            return Optional.empty();
        } catch (IllegalAccessException e) {

            e.printStackTrace();
           return  Optional.empty();
        }
    }


    private static final DynamicType.Builder<?> createBeanClass(final String className, final Map<String, Object> properties){

        DynamicType.Builder<HopperTask> buddy = new ByteBuddy()
                .subclass(HopperTask.class)
                .name(className);

        for(Map.Entry<String, Object> propEntry : properties.entrySet()){

            Map<String, String> prop = (Map) propEntry.getValue();

            try {

                if(Boolean.parseBoolean(prop.get(TaskProperties.REQUIRED))) {

                        buddy = buddy.defineProperty(propEntry.getKey(), Class.forName(prop.get(TaskProperties.TYPE)))
                                .annotateField(AnnotationDescription.Builder.ofType(Required.class).build());

                }
                else{

                    buddy = buddy.defineProperty(propEntry.getKey(), Class.forName(prop.get(TaskProperties.TYPE)));
                }

            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        return buddy;
    }

    private void updateRequiredProperties(String appName){

        List<String> requiredpropeties = new ArrayList<>();

        //TODO get it from base class without the need of create a new object.

        Field declaredFliends[]  = createTaskObject(appName).getClass().getDeclaredFields();

        for(int i = 0; i < declaredFliends.length; ++i){

            Annotation declaredAnnotations[] = declaredFliends[i].getDeclaredAnnotations();

            for (int p = 0; p < declaredAnnotations.length; ++p){

                if(declaredAnnotations[p].annotationType().equals(Required.class)){
                        requiredpropeties.add(declaredFliends[i].getName());
                }
            }

        }

        requiredpropeties.forEach((requiredProperty) -> requiredFieldsMap.put(appName,requiredProperty));
    }
}



