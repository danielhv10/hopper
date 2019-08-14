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
import model.HopperTask;
import org.junit.Before;
import org.junit.Test;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Map;

import static org.junit.Assert.*;

public class TaskControllerTest {

    private TaskController taskController;
    private HopperTask workerTask;

    @Before
    public void setUp() {
        taskController = new TaskController();

        workerTask = new HopperTask() {

            private String newAttribute1;
            private int newAttribute;

            @Required
            private int newRequiredAttribute;

            public String getNewAttribute1() {
                return newAttribute1;
            }

            public void setNewAttribute1(String newAttribute1) {
                this.newAttribute1 = newAttribute1;
            }

            public int getNewAttribute() {
                return newAttribute;
            }

            public void setNewAttribute(int newAttribute) {
                this.newAttribute = newAttribute;
            }

            public int getNewRequiredAttribute() {
                return newRequiredAttribute;
            }

            public void setNewRequiredAttribute(int newRequiredAttribute) {
                this.newRequiredAttribute = newRequiredAttribute;
            }
        };
    }


    @Test
    public void getTaskAttributes() {

        Field baseTaskFields[] = HopperTask.class.getDeclaredFields();
        Field workerTaskFields[] = workerTask.getClass().getDeclaredFields();

        Map<String, Object> testTaskFields = taskController.getTaskAttributes(workerTask.getClass());

        assertNotNull(workerTaskFields);

        assertEquals( baseTaskFields.length + workerTaskFields.length,
                workerTask.getClass().getDeclaredFields().length + workerTask.getClass().getSuperclass().getDeclaredFields().length);


        for(int i = 0; i < workerTaskFields.length; ++i){
            assertTrue(testTaskFields.containsKey(workerTaskFields[i].getName()));
        }
    }


    @Test
    public void GetrequiredAnnotation(){

        Field workerTaskFields[] = workerTask.getClass().getDeclaredFields();
        Annotation propertyAnnotations[] = null;

        boolean requiredFound = false;

        for(int i = 0; i < workerTaskFields.length; ++i){

            if(workerTaskFields[i].getName().equals("newRequiredAttribute")){

                propertyAnnotations =  workerTaskFields[i].getDeclaredAnnotations();

                for(int p = 0; p < propertyAnnotations.length; ++p){

                    if (propertyAnnotations[p].annotationType().equals(Required.class)){
                        requiredFound = true;
                        break;
                    }

                }
            }
            if(requiredFound){
                break;
            }
        }
        assertTrue(requiredFound);
    }
}