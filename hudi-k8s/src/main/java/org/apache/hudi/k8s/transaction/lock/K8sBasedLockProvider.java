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
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.lock.LockState;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieK8sBasedLockConfig;
import org.apache.hudi.exception.HoodieLockException;

import io.kubernetes.client.extended.leaderelection.LeaderElectionConfig;
import io.kubernetes.client.extended.leaderelection.LeaderElector;
import io.kubernetes.client.extended.leaderelection.resourcelock.LeaseLock;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Threads;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A Kubernetes based lock. This {@link LockProvider} implementation allows locking table operations
 * using Kubernetes endpoints. Users need to have access to Kubernetes cluster to be able to use this lock.
 */
@NotThreadSafe
public class K8sBasedLockProvider implements LockProvider<LeaseLock> {

  private static final Logger LOG = LogManager.getLogger(K8sBasedLockProvider.class);

  private final ApiClient client;
  private final boolean debugEnabled;
  private final String leaseNamespace;
  private final String leaseKey;
  private final String leaseHolder;
  private final long leaseDuration;
  private final long renewDeadline;
  private final long retryInterval;
  private final long shutdownTimeout;
  private final ExecutorService leaderElectorService;
  protected HoodieK8sBasedLockConfig lockConfiguration;
  private LeaderElector leaderElector;
  private final AtomicReference<LeaseLock> lock;

  public K8sBasedLockProvider(final LockConfiguration lockConfiguration, final Configuration conf) throws IOException {
    this(lockConfiguration, conf, null);
  }

  public K8sBasedLockProvider(final LockConfiguration lockConfiguration, final Configuration conf, ApiClient client) throws IOException {
    checkRequiredProps(lockConfiguration);
    HoodieK8sBasedLockConfig config = HoodieK8sBasedLockConfig.newBuilder().fromProperties(lockConfiguration.getConfig()).build();
    this.leaseNamespace = config.getString(HoodieK8sBasedLockConfig.K8S_LOCK_LEASE_NAMESPACE.key());
    this.leaseKey = config.getString(HoodieK8sBasedLockConfig.K8S_LOCK_LEASE_KEY.key());
    this.leaseHolder = config.getString(HoodieK8sBasedLockConfig.K8S_LOCK_LEASE_HOLDER_IDENTITY.key());
    this.leaseDuration = config.getLong(HoodieK8sBasedLockConfig.K8S_LOCK_LEASE_DURATION_MS);
    this.renewDeadline = config.getLong(HoodieK8sBasedLockConfig.K8S_LOCK_RENEW_DEADLINE_MS);
    this.retryInterval = config.getLong(HoodieK8sBasedLockConfig.K8S_LOCK_RETRY_INTERVAL_MS);
    this.shutdownTimeout = config.getLong(HoodieK8sBasedLockConfig.K8S_LOCK_SERVICE_SHUTDOWN_TIMEOUT_MS);
    this.debugEnabled = config.getBoolean(HoodieK8sBasedLockConfig.K8S_CLIENT_DEBUG_ENABLED);
    this.lockConfiguration = config;
    // build the Kubernetes client
    if (client != null) {
      this.client = client;
    } else {
      this.client = getK8sClient();
    }
    this.leaderElectorService = Executors.newSingleThreadExecutor(Threads.threadFactory("k8s-lock-provider-worker-%d"));
    this.lock = new AtomicReference<>();
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) {
    LOG.info(generateLogStatement(LockState.ACQUIRING, generateLogSuffixString()));
    try {
      ValidationUtils.checkArgument(this.lock.get() == null, generateLogStatement(LockState.ALREADY_ACQUIRED, generateLogSuffixString()));
      LeaseLock lock = new LeaseLock(leaseNamespace, leaseKey, leaseHolder, client);
      CountDownLatch countDownLatch = new CountDownLatch(1);
      LeaderElectionConfig leaderElectionConfig =
          new LeaderElectionConfig(
              lock, Duration.ofMillis(leaseDuration), Duration.ofMillis(renewDeadline), Duration.ofMillis(retryInterval));
      leaderElector = new LeaderElector(leaderElectionConfig);
      leaderElectorService.submit(() -> leaderElector.run(
          () -> {
            this.lock.set(lock);
            countDownLatch.countDown();
            LOG.info(generateLogStatement(LockState.ACQUIRED, generateLogSuffixString()));
          },
          () -> {
            LOG.info(generateLogStatement(LockState.RELEASED, generateLogSuffixString()));
          }));
      if (!countDownLatch.await(time, unit)) {
        LOG.info(generateLogStatement(LockState.FAILED_TO_ACQUIRE, generateLogSuffixString()));
        leaderElector.close();
        leaderElector = null;
        return false;
      }
    } catch (Exception e) {
      throw new HoodieLockException(generateLogStatement(LockState.FAILED_TO_ACQUIRE, generateLogSuffixString()), e);
    }
    return lock.get() != null;
  }

  @Override
  public void unlock() {
    try {
      if (lock.get() == null) {
        return;
      }
      if (leaderElector != null) {
        leaderElector.close();
        leaderElector = null;
        lock.set(null);
      }
    } catch (Exception e) {
      // TODO: Should throw new HoodieLockException ?
      LOG.error(generateLogStatement(LockState.FAILED_TO_RELEASE, generateLogSuffixString()));
    }
  }

  @Override
  public void close() {
    unlock();
    leaderElectorService.shutdown();
    boolean isTerminated = false;
    try {
      isTerminated = leaderElectorService.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ignored) {
      LOG.warn("Kubernetes leader election service close didn't finish.");
    }
    if (!isTerminated) {
      LOG.warn("Kubernetes leader election service close didn't finish.");
      return;
    }
    LOG.info("Kubernetes leader election service closed");
  }

  @Override
  public LeaseLock getLock() {
    return lock.get();
  }

  private ApiClient getK8sClient() throws IOException {
    String kubeconfigPath = this.lockConfiguration.getString(HoodieK8sBasedLockConfig.K8S_KUBECONFIG);
    String k8sURL = this.lockConfiguration.getString(HoodieK8sBasedLockConfig.K8S_URL);
    String authToken = this.lockConfiguration.getString(HoodieK8sBasedLockConfig.K8S_AUTH_TOKEN);
    boolean isValidateSSL = this.lockConfiguration.getBoolean(HoodieK8sBasedLockConfig.K8S_VALIDATE_SSL);
    ApiClient client;
    if (kubeconfigPath != null) {
      client = Config.fromConfig(kubeconfigPath).setDebugging(debugEnabled);
    } else if (k8sURL != null) {
      client = Config.fromToken(k8sURL, authToken, isValidateSSL).setDebugging(debugEnabled);
    } else {
      client = Config.defaultClient().setDebugging(debugEnabled);
    }
    return client;
  }

  private void checkRequiredProps(final LockConfiguration config) {
    ValidationUtils.checkArgument(config.getConfig().getString(HoodieK8sBasedLockConfig.K8S_LOCK_LEASE_NAMESPACE.key()) != null);
    ValidationUtils.checkArgument(config.getConfig().getString(HoodieK8sBasedLockConfig.K8S_LOCK_LEASE_KEY.key()) != null);
  }

  private String generateLogSuffixString() {
    return StringUtils.join("Lease namespace = ", leaseNamespace, ", lease key = ", leaseKey);
  }

  protected String generateLogStatement(LockState state, String suffix) {
    return StringUtils.join(state.name(), " lock at ", suffix);
  }
}
