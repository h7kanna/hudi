/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.k8s.transaction.lock;

import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.config.HoodieK8sBasedLockConfig;
import org.apache.hudi.exception.HoodieLockException;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY;

/**
 * Test for {@link K8sBasedLockProvider}.
 * Set it as integration test because it requires setting up Kubernetes kind cluster.
 */
public class ITTestK8sBasedLockProvider {

  private static LockConfiguration lockConfiguration;
  private static ApiClient apiClient;

  @BeforeAll
  public static void setup() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(HoodieK8sBasedLockConfig.K8S_LOCK_LEASE_KEY.key(), "test-key");
    properties.setProperty(HoodieK8sBasedLockConfig.K8S_LOCK_LEASE_NAMESPACE.key(), "test-namespace");
    properties.setProperty(HoodieK8sBasedLockConfig.K8S_LOCK_LEASE_HOLDER_IDENTITY.key(), "test-holder");
    properties.setProperty(HoodieK8sBasedLockConfig.K8S_CLIENT_DEBUG_ENABLED.key(), "true");
    properties.setProperty(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "60000");
    lockConfiguration = new LockConfiguration(properties);
    apiClient = getK8sClientWithKindCluster();
  }

  @Test
  public void testAcquireLock() throws IOException {
    K8sBasedLockProvider k8SBasedLockProvider = new K8sBasedLockProvider(lockConfiguration, null, null);
    Assertions.assertTrue(k8SBasedLockProvider.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    k8SBasedLockProvider.unlock();
  }

  @Test
  public void testUnlock() throws IOException {
    K8sBasedLockProvider k8SBasedLockProvider = new K8sBasedLockProvider(lockConfiguration, null, apiClient);
    Assertions.assertTrue(k8SBasedLockProvider.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    k8SBasedLockProvider.unlock();
    Assertions.assertTrue(k8SBasedLockProvider.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
  }

  @Test
  public void testReentrantLock() throws IOException {
    K8sBasedLockProvider k8SBasedLockProvider = new K8sBasedLockProvider(lockConfiguration, null, apiClient);
    Assertions.assertTrue(k8SBasedLockProvider.tryLock(lockConfiguration.getConfig()
        .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    try {
      k8SBasedLockProvider.tryLock(lockConfiguration.getConfig()
          .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS);
      Assertions.fail();
    } catch (HoodieLockException e) {
      // expected
    }
    k8SBasedLockProvider.unlock();
  }

  @Test
  public void testUnlockWithoutLock() throws IOException {
    K8sBasedLockProvider k8SBasedLockProvider = new K8sBasedLockProvider(lockConfiguration, null, apiClient);
    k8SBasedLockProvider.unlock();
  }

  private static ApiClient getK8sClientWithKindCluster() throws IOException {
    String url = System.getProperty("kubernetes-local.url");
    if (url != null && !url.isEmpty()) {
      String token = System.getProperty("kubernetes-local.token");
      if (token == null || token.isEmpty()) {
        throw new IllegalStateException("kubernetes-local.token system property not set");
      }
      return Config.fromToken(url, token, true).setDebugging(true);
    } else {
      return Config.defaultClient().setDebugging(true);
    }
  }

}
