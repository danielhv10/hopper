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

import model.Required;
import model.TaskProperties;
import model.ZooTask;
import net.bytebuddy.ByteBuddy;

import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
//TODO change the static approaching

public class TaskAPIController {

    private final static Logger LOG = Logger.getLogger(TaskAPIController.class);
    public static DynamicType.Builder<?> taskModel;
    public static String APP_NAME;
    private static List<String> requiredFields;

    public static void updateAPI(JSONObject taskJSON){

        LOG.info("New task model asigned");

        APP_NAME = taskJSON.getString(TaskProperties.APP_NAME);

        //TODO mach this two collections better

        Map<String, Object> props = new HashMap<String, Object>();


        taskJSON.getJSONObject(TaskProperties.PROPERTIES).toMap().forEach((k,v) -> {

                props.put(k, v);
            }
        );

        props.forEach((k,v) -> LOG.info(k + ", " + v));

        TaskAPIController.taskModel =  createBeanClass(taskJSON.getString(TaskProperties.CLASS_NAME), props);
        updateRequiredProperties();
    }

    //TODO improve null return error controlling
    public static Object createTaskObject(){
        try {

            return taskModel.make()
                    .load(ClassLoader.getSystemClassLoader(), ClassLoadingStrategy.Default.WRAPPER)
                    .getLoaded()
                    .newInstance();


        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static List<String> getRequiredFields(){
        return requiredFields;
    }

    private static final DynamicType.Builder<?> createBeanClass(final String className, final Map<String, Object> properties){

        DynamicType.Builder<ZooTask> buddy = new ByteBuddy()
                .subclass(ZooTask.class)
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

    private static void updateRequiredProperties(){

        List<String> requiredpropeties = new ArrayList<>();

        //TODO get it from base class without the need of create a new object.

        Field declaredFliends[]  = createTaskObject().getClass().getDeclaredFields();

        for(int i = 0; i < declaredFliends.length; ++i){

            Annotation declaredAnnotations[] = declaredFliends[i].getDeclaredAnnotations();

            for (int p = 0; p < declaredAnnotations.length; ++p){

                if(declaredAnnotations[p].annotationType().equals(Required.class)){
                        requiredpropeties.add(declaredFliends[i].getName());
                }
            }

        }

        requiredFields = requiredpropeties;
    }
}



