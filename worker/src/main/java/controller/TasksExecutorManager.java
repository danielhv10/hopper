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

import com.fasterxml.jackson.databind.ObjectMapper;
import model.HopperTask;
import org.apache.log4j.Logger;
import util.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class TasksExecutorManager {

    private final static Logger LOG = Logger.getLogger(TasksExecutorManager.class);

    private final WorkerTasksController workerTasksController;

    public interface TasksListener {

        void onCompleted(Tuple<String, Tuple<String, Boolean>> result);
    }

    private static volatile TasksExecutorManager INSTANCE = null;
    private ExecutorService pool = null;
    //TODO not used var
    private int threads;

    private final ArrayList<TasksListener> tasksListeners = new ArrayList<>();

    private TasksExecutorManager(WorkerTasksController workerTasksController) {
        this.workerTasksController = workerTasksController;
    }

    public static TasksExecutorManager getInstance(WorkerTasksController workerTasksController) {
        if (INSTANCE == null) {
            synchronized (TasksExecutorManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new TasksExecutorManager(workerTasksController);
                }
            }
        }
        return INSTANCE;
    }

    public void setThreads(int threads) {
        this.threads = threads;
        pool = Executors.newFixedThreadPool(threads);
    }

    public void addTasksListener(TasksListener listener) {
        tasksListeners.add(listener);
    }

    public void removeTasksListener(TasksListener listener) {
        tasksListeners.remove(listener);
    }

    //TODO FIXME Ver qué hacemos con esas excepciones(control in zookeeperValue)
    public void submit(byte[] serializedTaskObject, final Class taskExecutor, final Class taskModel) throws IllegalAccessException, InstantiationException, IOException {
        if (pool == null) {
            throw new RuntimeException("'setThreads()' must be called first");
        }

        TaskExecutor workerTasksControllerListener = (TaskExecutor) taskExecutor.newInstance();
        Object upcastedTask = new ObjectMapper().readValue(serializedTaskObject, taskModel);

        CompletableFuture.supplyAsync(new Supplier<Tuple<String, Tuple<String, Boolean>>>() {

            @Override
            public Tuple<String, Tuple<String, Boolean>> get() {
                return new Tuple<>(new String(serializedTaskObject), workerTasksControllerListener.doTask(upcastedTask));
            }
        }, pool).thenAccept(new Consumer<Tuple<String, Tuple<String, Boolean>>>() {

            @Override
            public void accept(Tuple<String, Tuple<String, Boolean>> result) {

                LOG.info("Task finished");

                workerTasksController.taskDone(((HopperTask)upcastedTask).getId());

                tasksListeners.forEach(l -> {
                    l.onCompleted(result);
                });
            }
        });
    }


}
