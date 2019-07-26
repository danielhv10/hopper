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

import model.Required;
import model.TaskProperties;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class TaskController {


    public Map<String, Object> getTaskAttributes(Class workerTask){

        Map<String, Object> propsIndex = new HashMap<String, Object>();

        Map<String, String> props = null;


        Field workerTaskFields[] = workerTask.getDeclaredFields();


        for (int i = 0; i < workerTaskFields.length; ++i) {

            props = new HashMap<String, String>();

            props.put(TaskProperties.TYPE, workerTaskFields[i].getType().getTypeName());
            props.put(TaskProperties.REQUIRED, workerTaskFields[i].getDeclaredAnnotation(Required.class) != null ? Boolean.toString(true): Boolean.toString(false));

            propsIndex.put(workerTaskFields[i].getName(), props);
        }

        return propsIndex;
    }
}
