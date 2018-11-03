/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.framework.services.kubernetes;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;


import io.fabric8.kubernetes.client.KubernetesClientException;
import lombok.extern.slf4j.Slf4j;


import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


@Slf4j
public class ZookeeperKubernetesService extends KubernetesBasedService {

    io.fabric8.kubernetes.client.Config config = new ConfigBuilder().build();
    KubernetesClient client = new DefaultKubernetesClient(config);
    private String deployment;

    public ZookeeperKubernetesService(String id) {
        super(id);
        log.info("Starting Zookeeper Service: {}", getID());


    }
    public ZookeeperKubernetesService(final String id, int instances, double cpu, double mem) {
        super(id);
    }

    @Override
    public void start(final boolean wait) {

        log.info("Starting Zookeeper Service: {}", getID());
        io.fabric8.kubernetes.client.Config config = new ConfigBuilder().build();
        KubernetesClient client = new DefaultKubernetesClient(config);


        try {
            // Create a namespace for all our stuff
            Namespace ns = new NamespaceBuilder().withNewMetadata().withName("nautilus").addToLabels("this", "rocks").endMetadata().build();
            log.info("Created namespace", client.namespaces().createOrReplace(ns));

            ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();

            client.serviceAccounts().inNamespace("nautilus").createOrReplace(fabric8);
            for (int i = 0; i < 1; i++) {
                System.err.println("Iteration:" + (i+1));
                Deployment deployment = new DeploymentBuilder()
                        .withNewMetadata()
                        .withName("zookeeper")
                        .endMetadata()
                        .withNewSpec()
                        .withReplicas(1)
                        .withNewTemplate()
                        .withNewMetadata()
                        .addToLabels("app", "zookeeper")
                        .endMetadata()
                        .withNewSpec()
                        .addNewContainer()
                        .withName("zookeeper")
                        .withImage("jplock/zookeeper:3.5.2-alpha")
                        .addNewPort()
                        .withContainerPort(2181)
                        .endPort()
                        .endContainer()
                        .endSpec()
                        .endTemplate()
                        .withNewSelector()
                        .addToMatchLabels("app", "zookeeper")
                        .endSelector()
                        .endSpec()
                        .build();

                deployment = client.apps().deployments().inNamespace("nautilus").create(deployment);
                log.info("Created deployment", deployment);

            }
            log.info("Done.");

        }catch (KubernetesClientException e) {
            log.error(e.getMessage());
            log.error("Failed to create a service, check if the service already exists", e.getMessage());
        }
    }
    private static void log(String action, Object obj) {
        log.info("{}: {}", action, obj);
    }

    private static void log(String action) {
        log.info(action);
    }


    @Override
    public List<URI> getServiceDetails() {

       List<Endpoints> result = client.endpoints().inNamespace("nautilus").list().getItems();

        System.out.println("Client:" + client.endpoints().inNamespace("nautilus").list().getItems());
        System.out.println("result:" + result);

        for (int i = 0; i < result.size(); i++) {
            System.out.println(result.get(i));
        }

        List<URI> uriList = new ArrayList<>();
        int port = 2181;
        URI uri;
        String ip = "";
        for (Endpoints ep : result) {

            List<EndpointSubset> subsetList = ep.getSubsets();

            for (EndpointSubset ss : subsetList) {
                for (Iterator<EndpointAddress> i = ss.getAddresses().iterator(); ((Iterator) i).hasNext(); ) {
                    EndpointAddress item = i.next();
                    uriList.add(URI.create("tcp://" + item.getIp() + ":" + port));
                    System.out.println("test:" + item.getIp());
                }
            }
        }

        for (int i = 0; i < uriList.size(); i++) {
            System.out.println("urilist:" + uriList.get(i));
        }
        return uriList;
    }

    //This is a placeholder to perform clean up actions
        @Override
        public void clean() {
        }

        @Override
        public void stop() {
            log.info("Stopping Zookeeper Service : {}", getID());
            client.resource(deployment).delete();
        }


}
