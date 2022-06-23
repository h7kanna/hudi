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

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.table.HoodieTableConfig;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieK8sBasedLockConfig {

  @Test
  public void testBuild() {
    HoodieConfig cfg = new HoodieConfig();
    cfg.setValue(HoodieTableConfig.NAME, "random-lease-key");
    cfg.setValue(HoodieK8sBasedLockConfig.K8S_LOCK_LEASE_NAMESPACE, "random-lease-namespace");
    HoodieK8sBasedLockConfig config = HoodieK8sBasedLockConfig.newBuilder().fromProperties(cfg.getProps()).build();
    assertEquals("random-lease-key", config.getString(HoodieK8sBasedLockConfig.K8S_LOCK_LEASE_KEY));
    assertEquals("random-lease-namespace", config.getString(HoodieK8sBasedLockConfig.K8S_LOCK_LEASE_NAMESPACE));
    assertEquals("apache-hudi-deltastreamer", config.getString(HoodieK8sBasedLockConfig.K8S_LOCK_LEASE_HOLDER_IDENTITY));
    assertEquals("60000", config.getString(HoodieK8sBasedLockConfig.K8S_LOCK_LEASE_DURATION_MS));
    assertEquals("30000", config.getString(HoodieK8sBasedLockConfig.K8S_LOCK_RENEW_DEADLINE_MS));
    assertEquals("5000", config.getString(HoodieK8sBasedLockConfig.K8S_LOCK_RETRY_INTERVAL_MS));
    assertEquals("10000", config.getString(HoodieK8sBasedLockConfig.K8S_LOCK_SERVICE_SHUTDOWN_TIMEOUT_MS));

    cfg = new HoodieConfig();
    cfg.setValue(HoodieK8sBasedLockConfig.K8S_LOCK_LEASE_KEY, "set-lease-key");
    config = HoodieK8sBasedLockConfig.newBuilder().fromProperties(cfg.getProps()).build();
    assertEquals("set-lease-key", config.getString(HoodieK8sBasedLockConfig.K8S_LOCK_LEASE_KEY));
  }
}
