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

import cache.TaskModelCache;
import controller.AssignTaskController;
import controller.DeleteTaskController;
import controller.TasksController;
import controller.WorkersController;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import util.PropertiesLoader;
import zookeeper.ZooPathTree;
import zookeeper.ZooServerConnection;
import zookeeper.ZookeeperEntity;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Properties;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Leader implements ZookeeperEntity {

    public WorkersController workersController;
    public TasksController tasksController;
    public AssignTaskController assignTaskController;
    public DeleteTaskController deleteTaskController;
    private MasterStates state;
    private final ZooKeeper zk;
    private TaskModelCache taskModelCache;
    private final static Logger LOG = Logger.getLogger(main.Leader.class);


    public static void main(String... args) throws IOException, InterruptedException, KeeperException {


        for(String arg: args){

            switch (arg){

                case InitArguments.DEVELOPMENT:

                    //Delete zookeeper server directory
                    try {

                        Files.walk(new File(System.getProperty("java.io.tmpdir").concat("zookeperDataTemp")).toPath())
                                .sorted(Comparator.reverseOrder())
                                .map(Path::toFile)
                                .forEach(File::delete);

                    } catch (NoSuchFileException e){

                        LOG.info("ZookeeperDataTem doesn't exists");
                    }

                    Path zookeeperDataDir = Files.createTempDirectory("zookeperDataTemp");

                    LOG.info("Starting zookeeper server development instance");
                    ZooKeeperServerMain zooKeeperServer = new ZooKeeperServerMain();

                    Properties properties = new Properties();
                    properties.setProperty("clientPort","2181");
                    properties.setProperty("dataDir", zookeeperDataDir.toString());
                    properties.setProperty("tickTime", Integer.toString(1000));

                    QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();


                    try {
                        quorumConfiguration.parseProperties(properties);
                    } catch(Exception e) {
                        throw new RuntimeException(e);
                    }

                    final ServerConfig configuration = new ServerConfig();
                    configuration.readFrom(quorumConfiguration);

                    new Thread(()-> {


                        try {
                            zooKeeperServer.runFromConfig(configuration);


                        } catch (IOException e) {
                            LOG.error("ZooKeeper server  Failed", e);
                        }

                        try {

                            Thread.sleep(Long.MAX_VALUE);

                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }).start();

                default:
            }
        }


        Leader m = new Leader();

        m.runForMaster();
        m.leaderExists();


        Thread.sleep(Long.MAX_VALUE);
    }


    public Leader() throws IOException {

        state = MasterStates.START;
        String zookeeperHost = null;
        int zookeeperPort = 0;

        HashSet<String> expectedProperties = new HashSet<>();
        expectedProperties.add("zookeeper.host");
        expectedProperties.add("zookeeper.port");

        Properties properties = PropertiesLoader.loadProperties(expectedProperties, '.', LOG);

        zookeeperHost = properties.getProperty("zookeeper.host");
        zookeeperPort = Integer.parseInt(properties.getProperty("zookeeper.port"));

        //properties.forEach((k, v) -> System.out.println(k + " " + v));

        this.zk = ZooServerConnection.getInstance(zookeeperHost,zookeeperPort).getZookeeperConnection();
    }

    public void leaderExists(){

        //Watcher to manage zookeeper state change:
        Watcher masterExistsWatcher = new Watcher(){


            public void process(WatchedEvent watchedEvent) {

                if(watchedEvent.getType() == Event.EventType.NodeDeleted){
                    assert ZooPathTree.MASTER.equals( watchedEvent.getPath());

                    try {
                        runForMaster();
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };

        zk.exists( ZooPathTree.MASTER, masterExistsWatcher, new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int i, String s, Object o, Stat stat) {
                switch(KeeperException.Code.get(i)){

                    case CONNECTIONLOSS:

                        leaderExists();
                        break;

                    case OK:

                        break;

                    case NONODE:

                        setState(MasterStates.RUNNING);

                        try {
                            runForMaster();

                            //TODO control exceptions
                        } catch (KeeperException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        break;

                    default:
                        checkmaster();
                        break;
                }
            }
        }, null );
    }


    //TODO control exceptions
    public void runForMaster() throws KeeperException, InterruptedException {

        LOG.info(new StringBuilder().append(ZookeeperEntity.SERVER_ID).append(" : ").append("running for master"));
        LOG.info(state.toString());

        zk.create(ZooPathTree.MASTER, ZookeeperEntity.SERVER_ID.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int i, String s, Object o, String s1) {
                switch(KeeperException.Code.get(i)){

                    case CONNECTIONLOSS:
                        checkmaster();
                        break;

                    case OK:

                        LOG.info(new StringBuilder().append(ZookeeperEntity.SERVER_ID).append(" : ").append("I'm MASTER"));
                        setState(MasterStates.ELECTED);

                        bootstrap();

                        //subscribe to taskmodel znode.
                        taskModelCache = new TaskModelCache();
                        break;

                    case NODEEXISTS:
                        LOG.info(new StringBuilder(). append(ZookeeperEntity.SERVER_ID).append(" : ").append("I'm NOT MASTER"));
                        setState(MasterStates.NOTELECTED);
                        leaderExists();
                        break;

                    default:

                        setState(MasterStates.NOTELECTED);
                        LOG.error("something went wrong when running for master", KeeperException.create(KeeperException.Code.get(i), s));

                }
            }
        }, this);
    }


    /**
     * returns true if there is a master
     */
    public void checkmaster(){

      zk.getData( ZooPathTree.MASTER, false , new AsyncCallback.DataCallback() {
          @Override
          public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {
              switch (KeeperException.Code.get(i)) {
                  case CONNECTIONLOSS:
                      checkmaster();
                      return;

                  case NONODE:

                      try {

                          runForMaster();

                          //TODO control exceptions
                      } catch (KeeperException e) {
                          e.printStackTrace();
                      } catch (InterruptedException e) {
                          e.printStackTrace();
                      }
                      return;

              }
          }
      }, null);

    }

    public void createParent(String path, byte[] data){
        zk.create(path, data, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int i, String s, Object o, String s1) {

                switch (KeeperException.Code.get(i)){
                    case CONNECTIONLOSS:
                        createParent(s,(byte[]) o);
                        break;

                    case OK:
                        LOG.info( new StringBuilder().append("Parent ").append(s).append(" Created"));
                        break;

                    case NODEEXISTS:
                        LOG.info("parent aready registered " + s);
                        break;

                    case NONODE:
                        createParent(s,(byte[]) o);
                        break;
                    default:
                        LOG.error("something went wrong: " + KeeperException.create(KeeperException.Code.get(i), s));

                }
            }
        }, data);
    }

    /**
     * Creates zookeeper dir tree
     */
    public void bootstrap(){
        Long emptyInitDate = 0L;
        LOG.info("Creating subnodetree");
        createParent(ZooPathTree.BASE_PATH, new byte[0]);
        createParent(ZooPathTree.WORKERS, new byte[0]);
        createParent(ZooPathTree.ASSIGN, new byte[0]);
        createParent(ZooPathTree.TASKS, new byte[0]);
        createParent(ZooPathTree.STATUS, new byte[0]);
        createParent(ZooPathTree.TASK_MODEL, new byte[0]);
        createParent(ZooPathTree.TASK_DELETE, new byte[0]);
    }

    /**
     *
     * @return current master status
     */
    public MasterStates getMasterState(){
        return state;
    }

    public MasterStates getState() {
        return state;
    }

    public void setState(MasterStates state) {
        this.state = state;
    }
}
