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

import model.HopperTask;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;

class TasksExecutorManager {

    class VerboseScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {

        VerboseScheduledThreadPoolExecutor(int i) {

            super(i);
        }

        long getQueueHeadDelay(TimeUnit timeUnit) {
            Delayed head = (Delayed) super.getQueue().peek();
            if (head == null) {
                return 0;
            }
            return head.getDelay(timeUnit);
        }
    }

    @FunctionalInterface
    interface TasksListener {

        void onCompleted(String taskId);
    }



    private final static Logger LOG = Logger.getLogger(TasksExecutorManager.class);

    private static volatile TasksExecutorManager INSTANCE = null;

    private final VerboseScheduledThreadPoolExecutor verboseScheduledPool;
    private final ExecutorService callbackPool;

    private final HashSet<TasksListener> tasksListeners = new HashSet<>();
    private final Hashtable<String, ScheduledFuture> currentTasks = new Hashtable<>();

    //Teoricamente al ser solo para lectura y ser un solo hilo el que lo actualiza, se nos garantiza la última copia en memoria.
    private volatile long inHeadTaskDelay = 0;
    private static final short STATISTICS_REFRESH_MILLIS = 1000;
    private final Timer timer;


    private TasksExecutorManager() {
        callbackPool = Executors.newCachedThreadPool();
        verboseScheduledPool = new VerboseScheduledThreadPoolExecutor(deduceCorePoolSize());
        timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                inHeadTaskDelay = verboseScheduledPool.getQueueHeadDelay(TimeUnit.MILLISECONDS);
            }
        }, 0, STATISTICS_REFRESH_MILLIS);
    }

    static TasksExecutorManager getInstance() {
        if (INSTANCE == null) {
            synchronized (TasksExecutorManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new TasksExecutorManager();
                }
            }
        }
        return INSTANCE;
    }

    private int deduceCorePoolSize() {
        return Runtime.getRuntime().availableProcessors();
    }

    void addTasksListener(TasksListener listener) {
        tasksListeners.add(listener);
    }

    void removeTasksListener(TasksListener listener) {
        tasksListeners.remove(listener);
    }

    void submitTask(final HopperTask task, final Class taskExecutor) throws IllegalAccessException, InstantiationException {

        TaskExecutor workerTaskExecutor = (TaskExecutor) taskExecutor.newInstance();

        CompletableFuture.runAsync(() -> {
            long period = task.getPollingInterval();
            ScheduledFuture auxScheduledFuture;
            if (period <= 0) {
                auxScheduledFuture = verboseScheduledPool.schedule(() -> workerTaskExecutor.doTask(task), 0, TimeUnit.MILLISECONDS);
            } else {
                auxScheduledFuture = verboseScheduledPool.scheduleWithFixedDelay(() -> workerTaskExecutor.doTask(task), 0, period, TimeUnit.MILLISECONDS);
            }
            currentTasks.put(task.getId(), auxScheduledFuture);
            try {
                auxScheduledFuture.get();
                tasksListeners.forEach(l -> l.onCompleted(task.getId()));
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();//TODO Gestionar excepción
            }
        }, callbackPool);
    }

    boolean cancelTask(String taskId, boolean mayInterruptIfRunning) {
        boolean canceled;
        ScheduledFuture auxFuture = currentTasks.get(taskId);
        if (auxFuture != null) {
            //Si "cancel()" falla es porque o ya se canceló o está completada; también puede fallar por alguna otra cosa, pero eso no lo controlamos.
            if (!(canceled = auxFuture.cancel(mayInterruptIfRunning))) {
                //Si fue cancelada o está hecha, damos por cancelada
                if (auxFuture.isCancelled() || auxFuture.isDone()) {
                    canceled = true;
                }
            }
        } else {
            //Si la tarea no existe, devuelve como que está cancelada
            canceled = true;
        }
        return canceled;
    }

    long getInHeadTaskDelay() {
        return inHeadTaskDelay;
    }
}
