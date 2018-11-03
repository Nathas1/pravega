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
import java.net.URI;
import java.util.*;
import io.pravega.test.system.framework.services.kubernetes.KubernetesBasedService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BookkeeperKubernetesService extends KubernetesBasedService {

    private static final int BK_PORT = 3181;
    private final URI zkUri;
    private int instances = 3;
    private double cpu = 0.5;
    private double mem = 1024.0;
    private String deployment;

    public BookkeeperKubernetesService(final String id, final URI zkUri) {
        // if SkipserviceInstallation flag is enabled used the default id.
        super(id);
        this.zkUri = zkUri;

        io.fabric8.kubernetes.client.Config config = new ConfigBuilder().build();
        KubernetesClient client = new DefaultKubernetesClient(config);
        try {
            // Create a namespace for all our stuff
            Namespace ns = new NamespaceBuilder().withNewMetadata().withName("nautilus").addToLabels("this", "rocks").endMetadata().build();
            //log("Created namespace", client.namespaces().createOrReplace(ns));
            log.info("Created namespace", client.namespaces().createOrReplace(ns));

            ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();

            client.serviceAccounts().inNamespace("nautilus").createOrReplace(fabric8);
            for (int i = 0; i < 1; i++) {
                System.err.println("Iteration:" + (i+1));
                Deployment deployment = new DeploymentBuilder()
                        .withNewMetadata()
                        .withName("bookkeeper")
                        .endMetadata()
                        .withNewSpec()
                        .withReplicas(1)
                        .withNewTemplate()
                        .withNewMetadata()
                        .addToLabels("app", "bookkeeper")
                        .endMetadata()
                        .withNewSpec()
                        .addNewContainer()
                        .withName("bookkeeper")
                        .withImage("pravega/bookkeeper")
                        .addNewPort()
                        .withContainerPort(3181)
                        .endPort()
                        .endContainer()
                        .endSpec()
                        .endTemplate()
                        .withNewSelector()
                        .addToMatchLabels("app", "bookkeeper")
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

    public BookkeeperKubernetesService(final String id, final URI zkUri, int instances, double cpu, double mem) {
        // if SkipserviceInstallation flag is enabled used the default id.
        super(id);
        this.zkUri = zkUri;
        this.instances = instances;
        this.cpu = cpu;
        this.mem = mem;
    }

    @Override
    public void start(final boolean wait) {

    }

    //This is a placeholder to perform clean up actions
    @Override
    public void clean() {
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
        int port = 3181;
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

    @Override
    public void stop() {
        log.info("Stopping Zookeeper Service : {}", getID());
        client.resource(deployment).delete();
        
    }


}
