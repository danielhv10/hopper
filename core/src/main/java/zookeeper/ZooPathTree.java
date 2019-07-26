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

package zookeeper;

public class ZooPathTree  {

    static final public String BASE_PATH = "/twitterConsumer";

    static final public String MASTER   = "/master-TwitterConsumer";

    static final public String WORKERS = BASE_PATH.concat("/workers");
    static final public String ASSIGN  = BASE_PATH.concat("/assign");
    static final public String TASKS   = BASE_PATH.concat("/tasks");
    static final public String STATUS  = BASE_PATH.concat("/status");
    static final public String TASK_MODEL   = BASE_PATH.concat("/model");
    static final public String TASK_DELETE  = BASE_PATH.concat("/delete");

    static final public String CLIENT_DATA_UPDATE = BASE_PATH.concat("/updateDate");

    static final public String ASSIGN_WORKER = BASE_PATH.concat("/assign/worker-");

}