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

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import zookeeper.Exception.ConnectionNotExistsException;
import zookeeper.Exception.ConnectionStablishedException;


public class ZooCuratorConnection {

    private final static Logger LOG = Logger.getLogger(ZooCuratorConnection.class);

    private static volatile ZooCuratorConnection instance = null;
    private RetryPolicy retryPolicy = null;
    private CuratorFramework curatorClient = null;

    public static synchronized ZooCuratorConnection getInstance(){

        if(instance == null){
            instance = new ZooCuratorConnection();
        }

        return instance;
    }

    //TODO manage asyc connection status into curator
    public synchronized void init(String zookeeperHost, int zookeeperPort) throws ConnectionStablishedException {

        if(curatorClient == null){

            retryPolicy = new ExponentialBackoffRetry(1000, 3);
            String zookeeperConnectionString = new StringBuilder(zookeeperHost).append(":").append(zookeeperPort).toString();
            curatorClient = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy);

            curatorClient.getUnhandledErrorListenable().addListener((message, e) -> {
                LOG.error("error=" + message);
                e.printStackTrace();
            });

            curatorClient.getConnectionStateListenable().addListener((c, newState) -> {
                LOG.info("state=" + newState);
            });

            curatorClient.start();

            try {

                Thread.sleep(1000);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }else{
            throw new ConnectionStablishedException("Connection already stablized");
        }
    }

    private ZooCuratorConnection(){

    }

    public CuratorFramework getCuratorClientConnection() throws ConnectionNotExistsException {

        if(curatorClient != null){

            return this.curatorClient;
        }
        else{
            throw new ConnectionNotExistsException("Connection not stablized");
        }
    }
}
