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

import API.status.DeleteTaskStatusResponse;
import API.status.GetTaskStatusResponse;
import model.exceptions.TaskModelException;
import org.apache.zookeeper.ZooKeeper;
import org.json.JSONObject;
import zookeeper.ZooBaseConnection;

import java.util.Optional;

public interface TaskAPIInterface {

    ZooKeeper zk = ZooBaseConnection.getInstance().getZookeeperConnection();

    public Optional<String> addTask(String appName, JSONObject jsonObject) throws TaskModelException;
    public GetTaskStatusResponse getTask(String taskId);
    public DeleteTaskStatusResponse deleteTask(String taskId);
}
