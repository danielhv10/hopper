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

package main;

import API.TaskAPI;
import cache.StatusCache;
import controller.ZooTaskController;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;
import util.PropertiesLoader;
import zookeeper.ZooServerConnection;
import zookeeper.ZookeeperEntity;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;

public class Client implements ZookeeperEntity {

    private final static Logger LOG = Logger.getLogger(ZooTaskController.class);

    private final ZooTaskController zooTaskController;
    protected final ZooKeeper zk;
    private ClientStates state;
    private TaskAPI taskAPI;

    public static void main(String[] args) throws IOException, InterruptedException {

        new Client();

       StatusCache statusCache =  StatusCache.getInstance();

        //TODO delete this endless race
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Client() throws IOException {

        state = ClientStates.START;

        String zookeeperHost = null;
        int zookeeperPort = 0;

        HashSet<String> expectedProperties = new HashSet<>();
        expectedProperties.add("zookeeper.host");
        expectedProperties.add("zookeeper.port");

        Properties properties = PropertiesLoader.loadProperties(expectedProperties, '.', LOG);

        zookeeperHost = properties.getProperty("zookeeper.host");
        zookeeperPort = Integer.parseInt(properties.getProperty("zookeeper.port"));

        this.zk = ZooServerConnection.getInstance(zookeeperHost,zookeeperPort).getZookeeperConnection();
        this.zooTaskController = new ZooTaskController();

        this.taskAPI =  TaskAPI.getInstance();

        zooTaskController.getTasks();
    }


    /**
     *
     * @return current client status
     */
    public ClientStates getMasterState(){
        return state;
    }

    public ClientStates getState() {
        return state;
    }

    public void setState(ClientStates state) {
        this.state = state;
    }
}

