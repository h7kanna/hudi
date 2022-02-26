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

package org.apache.hudi.config;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import static org.apache.hudi.common.config.LockConfiguration.LOCK_PREFIX;

/**
 * Hoodie Configs for Locks.
 */
@ConfigClassProperty(name = "Kubernetes based Locks Configurations",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configs that control Kubernetes based locking mechanisms required for concurrency control "
        + " between writers to a Hudi table. Concurrency between Hudi's own table services "
        + " are auto managed internally. Refer https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/lease-v1/")
public class HoodieK8sBasedLockConfig extends HoodieConfig {

  // configs for Kubernetes based locks
  public static final String KUBERNETES_BASED_LOCK_PROPERTY_PREFIX = LOCK_PREFIX + "k8s.";

  public static final ConfigProperty<String> K8S_LOCK_LEASE_NAMESPACE = ConfigProperty
      .key(KUBERNETES_BASED_LOCK_PROPERTY_PREFIX + "lease_namespace")
      .defaultValue("default")
      .sinceVersion("0.11.0")
      .withDocumentation("For Kubernetes based lock provider, the name of the lease namespace");

  public static final ConfigProperty<String> K8S_LOCK_LEASE_KEY = ConfigProperty
      .key(KUBERNETES_BASED_LOCK_PROPERTY_PREFIX + "lease_key")
      .defaultValue("hudi")
      .sinceVersion("0.11.0")
      .withInferFunction(cfg -> {
        if (cfg.contains(HoodieTableConfig.NAME)) {
          return Option.of(cfg.getString(HoodieTableConfig.NAME));
        }
        return Option.empty();
      })
      .withDocumentation("For Kubernetes based lock provider, the lease key for the lock. "
          + "Each Hudi dataset should has it's unique key so concurrent writers could refer to the lease key."
          + " By default we use the Hudi table name specified to be the lease key");

  public static final ConfigProperty<String> K8S_LOCK_LEASE_HOLDER_IDENTITY = ConfigProperty
      .key(KUBERNETES_BASED_LOCK_PROPERTY_PREFIX + "lease_holder_identity")
      .defaultValue("apache-hudi-deltastreamer")
      .sinceVersion("0.11.0")
      .withInferFunction(cfg -> {
        String lockHolderIdentityName = System.getenv("HOSTNAME");
        if (lockHolderIdentityName != null) {
          return Option.of(lockHolderIdentityName);
        }
        return Option.empty();
      })
      .withDocumentation("For Kubernetes based lock provider, the lease holder identity. "
          + "Kubernetes lock service needs an identity of the lock holder."
          + " By default, if not provided HOSTNAME environment variable will be considered first or else it will be 'apache-hudi-deltastreamer'");

  public static final ConfigProperty<String> K8S_LOCK_LEASE_DURATION_MS = ConfigProperty
      .key(KUBERNETES_BASED_LOCK_PROPERTY_PREFIX + "lease_duration_ms")
      .defaultValue("60000")
      .sinceVersion("0.11.0")
      .withDocumentation("For Kubernetes based lock provider, the lease duration in milliseconds");

  public static final ConfigProperty<String> K8S_LOCK_RENEW_DEADLINE_MS = ConfigProperty
      .key(KUBERNETES_BASED_LOCK_PROPERTY_PREFIX + "renew_deadline_ms")
      .defaultValue("30000")
      .sinceVersion("0.11.0")
      .withDocumentation("For Kubernetes based lock provider, the renew deadline in milliseconds");

  public static final ConfigProperty<String> K8S_LOCK_RETRY_INTERVAL_MS = ConfigProperty
      .key(KUBERNETES_BASED_LOCK_PROPERTY_PREFIX + "retry_interval_ms")
      .defaultValue("5000")
      .sinceVersion("0.11.0")
      .withDocumentation("For Kubernetes based lock provider, the renew retry interval in milliseconds");

  public static final ConfigProperty<String> K8S_LOCK_SERVICE_SHUTDOWN_TIMEOUT_MS = ConfigProperty
      .key(KUBERNETES_BASED_LOCK_PROPERTY_PREFIX + "service_shutdown_timeout_ms")
      .defaultValue("10000")
      .sinceVersion("0.11.0")
      .withDocumentation("For Kubernetes based lock provider, the lock executor service shutdown timeout in milliseconds");

  public static final ConfigProperty<String> K8S_KUBECONFIG = ConfigProperty
      .key(KUBERNETES_BASED_LOCK_PROPERTY_PREFIX + "kubeconfig")
      .noDefaultValue()
      .sinceVersion("0.11.0")
      .withDocumentation("For Kubernetes based lock provider, the KUBECONFIG file path used for Kubernetes service."
          + " Useful for development with a local Kubernetes instance.");

  public static final ConfigProperty<String> K8S_URL = ConfigProperty
      .key(KUBERNETES_BASED_LOCK_PROPERTY_PREFIX + "k8s_url")
      .noDefaultValue()
      .sinceVersion("0.11.0")
      .withDocumentation("For Kubernetes based lock provider, the kubernetes url"
          + " Useful for development with a local Kubernetes instance.");

  public static final ConfigProperty<String> K8S_AUTH_TOKEN = ConfigProperty
      .key(KUBERNETES_BASED_LOCK_PROPERTY_PREFIX + "auth_token")
      .noDefaultValue()
      .sinceVersion("0.11.0")
      .withDocumentation("For Kubernetes based lock provider, the authentication token"
          + " Useful for development with a local Kubernetes instance.");

  public static final ConfigProperty<String> K8S_VALIDATE_SSL = ConfigProperty
      .key(KUBERNETES_BASED_LOCK_PROPERTY_PREFIX + "validate_ssl")
      .defaultValue("true")
      .sinceVersion("0.11.0")
      .withDocumentation("For Kubernetes based lock provider, flag to whether validate SSL"
          + " Useful for development with a local Kubernetes instance.");

  public static final ConfigProperty<String> K8S_CLIENT_DEBUG_ENABLED = ConfigProperty
      .key(KUBERNETES_BASED_LOCK_PROPERTY_PREFIX + "debug_enabled")
      .defaultValue("false")
      .sinceVersion("0.11.0")
      .withDocumentation("For Kubernetes based lock provider, whether to enable debug mode for Kubernetes lease client."
          + " Useful for development and debugging.");

  // TODO: Add options for Kubernetes client http connection settings

  private HoodieK8sBasedLockConfig() {
    super();
  }

  public static HoodieK8sBasedLockConfig.Builder newBuilder() {
    return new HoodieK8sBasedLockConfig.Builder();
  }

  public static class Builder {

    private final HoodieK8sBasedLockConfig hoodieK8sBasedLockConfig = new HoodieK8sBasedLockConfig();

    public HoodieK8sBasedLockConfig.Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.hoodieK8sBasedLockConfig.getProps().load(reader);
        return this;
      }
    }

    public HoodieK8sBasedLockConfig.Builder fromProperties(Properties props) {
      this.hoodieK8sBasedLockConfig.getProps().putAll(props);
      return this;
    }

    public HoodieK8sBasedLockConfig build() {
      hoodieK8sBasedLockConfig.setDefaults(HoodieK8sBasedLockConfig.class.getName());
      return hoodieK8sBasedLockConfig;
    }
  }
}
