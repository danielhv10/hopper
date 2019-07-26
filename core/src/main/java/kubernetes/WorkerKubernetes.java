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

package kubernetes;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.ExtensionsV1beta1Api;
import io.kubernetes.client.models.*;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.KubeConfig;
import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;


import java.io.*;


public class WorkerKubernetes {

    private static final String KUBERNETES_CONFIG_FILE_NAME      = "kubeConfig";
    private static final String KUBERNENTES_DEPLOYMENT_FILE_NAME = "workers.yaml";
    private static final String NAMESPACE =   "default";
    private final static Logger LOG       = Logger.getLogger(WorkerKubernetes.class);

    private KubeConfig kubeConfig;
    private ApiClient client;
    private ExtensionsV1beta1Api v1BetaAPI;


    public WorkerKubernetes(){

        kubeConfig = chargeConfig();

        try {

            client = Config.fromConfig(kubeConfig);
            Configuration.setDefaultApiClient(client);
            v1BetaAPI = new ExtensionsV1beta1Api();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws IOException, ApiException {

        new WorkerKubernetes().createWorkersDeployment();


    }

    public void createWorkersDeployment(){

        ExtensionsV1beta1Deployment body = new WorkerKubernetes().getDeployment();

        try {

            ExtensionsV1beta1Deployment response =  v1BetaAPI.createNamespacedDeployment(NAMESPACE, body, "");

        } catch (ApiException e) {

            e.printStackTrace();
        }
    }

    public boolean workersDesploymentKubernetesExists(){
        //TODO
        return true;
    }

    public boolean getWorkersDeploymentStatus(){
        //TODO
        return true;
    }

    private  KubeConfig chargeConfig() {

        KubeConfig kubeConfig = null;

        FileReader fileReader = null;

        try{

            String path = "./".concat(KUBERNETES_CONFIG_FILE_NAME);
            LOG.info("getting properties from main path");

            fileReader = new FileReader(path);
            LOG.info("Main path chargued");


        } catch (Exception e) {


            LOG.info("properties didn't find trying from resources");

            try {

                fileReader = new FileReader(ClassLoader.getSystemResource(KUBERNETES_CONFIG_FILE_NAME).getFile());

            } catch (FileNotFoundException e1) {
                LOG.error(e1);
            }
        }


            kubeConfig = KubeConfig.loadKubeConfig(fileReader);


        try {

            fileReader.close();

        } catch (IOException e) {

            e.printStackTrace();
        }

        return kubeConfig;
    }


    public ExtensionsV1beta1Deployment getDeployment() {

        Yaml yaml = new Yaml();

        FileReader fileReader = null;

        try{

            String path = "./".concat(KUBERNENTES_DEPLOYMENT_FILE_NAME);
            LOG.info("getting properties from main path");

            fileReader = new FileReader(path);
            LOG.info("Main path chargued");

        } catch (Exception e) {

            LOG.info("properties didn't find trying from resources");

            try {

                fileReader = new FileReader(ClassLoader.getSystemResource(KUBERNENTES_DEPLOYMENT_FILE_NAME).getFile());

            } catch (FileNotFoundException e1) {
                LOG.error(e1);
            }
        }

            return  yaml.loadAs(fileReader, ExtensionsV1beta1Deployment.class);
    }

}
