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

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Random;


public final class ZooServerConnection implements org.apache.zookeeper.Watcher  {

    public final  String SERVER_ID = Integer.toHexString(new Random().nextInt());
    protected final static Logger LOG = Logger.getLogger(ZooServerConnection.class);

    protected final String hostServer;
    protected final int port;
    private ZooKeeper zk;

    private volatile boolean connected = false;
    private volatile boolean expired = false;

    private static ZooServerConnection instance = null;



    public static ZooServerConnection getInstance() {
        if(instance== null) {
            instance= new ZooServerConnection();
        }
        return instance;
    }

    public static ZooServerConnection getInstance(String hostServer, int port) {

        if(instance == null) {
            instance = new ZooServerConnection(hostServer, port);
        }
        return instance;
    }


    private ZooServerConnection(String hostServer, int port){
        this.hostServer = hostServer;
        this.port = port;

        startZK();
    }


    private ZooServerConnection(){
        this.hostServer = "localhost";
        this.port       = 2181;
        startZK();
    }


    //TODO manage multiple connection trys
    public void startZK(){
        int tryConnectionCount = 0;
        try {
            tryConnectionCount++;
            this.zk = new ZooKeeper(this.hostServer + ":" + this.port, 15000, this);

        } catch (IOException e) {
            LOG.error(e);
        }
    }

    public void stopZK() throws  InterruptedException {

        LOG.info( "Closing" );

        try{
            zk.close();
        } catch (InterruptedException e) {
            LOG.warn("ZooKeeper interrupted while closing");
        }
    }


    /**
     * Checks if this client is connected.
     *
     * @return boolean
     */
    public boolean isConnected() {
        return connected;
    }

    /**
     * Checks if ZooKeeper session is expired.
     *
     * @return
     */
    public boolean isExpired() {
        return expired;
    }


    @Override
    /**
     * Deals with session events like connecting
     * and disconnecting.
     *
     * @param e new event generated
     */
    public void process(WatchedEvent event) {

            LOG.info(event.toString() + ", " + hostServer + port);

            if(event.getType() == Event.EventType.None){
                switch (event.getState()) {
                    case SyncConnected:
                        /*
                         * Registered with ZooKeeper
                         */
                        connected = true;
                        break;
                    case Disconnected:
                        connected = false;
                        break;
                    case Expired:
                        expired = true;
                        connected = false;
                        LOG.error("Session expired");
                        break;
                    default:
                        LOG.error("session state not found");
                        break;

                }
            }
        }

        public ZooKeeper getZookeeperConnection(){
            return this.zk;
        }
}
