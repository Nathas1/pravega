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

import com.google.common.base.Preconditions;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.*;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.pravega.common.concurrent.Futures;
import io.pravega.test.system.framework.TestFrameworkException;
import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.pravega.test.system.framework.TestFrameworkException.Type.InternalError;
import static io.pravega.test.system.framework.TestFrameworkException.Type.RequestFailed;

/**
 * Marathon based service implementations.
 */
@Slf4j
public abstract class
KubernetesBasedService implements io.pravega.test.system.framework.services.Service {

    static final int ZKSERVICE_ZKPORT = 2181;
    private static final String TCP = "tcp://";
    final String id;


    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);
    //final KubernetesClient client = new DefaultKubernetesClient()


    io.fabric8.kubernetes.client.Config config = new ConfigBuilder().build();
    KubernetesClient client = new DefaultKubernetesClient(config);



    KubernetesBasedService(final String id) {
        this.id = id.toLowerCase(); //Marathon allows only lowercase ids.

    }
    private static void log(String action, Object obj) {
        log.info("{}: {}", action, obj);
    }

    private static void log(String action) {
        log.info(action);
    }

    @Override
    public String getID() {
        return this.id;
    }

    @Override
    public boolean isRunning() {
        return true; /* this is the check for correct client is running*/
    }

    @Override
    public CompletableFuture<Void> scaleService(final int instanceCount) {
          return null;
    }


}
