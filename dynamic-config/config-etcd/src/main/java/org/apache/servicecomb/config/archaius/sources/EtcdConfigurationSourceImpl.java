/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.servicecomb.config.archaius.sources;

import static com.netflix.config.WatchedUpdateResult.createIncremental;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.configuration.Configuration;
import org.apache.servicecomb.config.ConfigMapping;
import org.apache.servicecomb.config.spi.ConfigCenterConfigurationSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.netflix.config.WatchedUpdateListener;
import com.netflix.config.WatchedUpdateResult;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;

public class EtcdConfigurationSourceImpl implements ConfigCenterConfigurationSource {
  private static final Logger LOGGER = LoggerFactory.getLogger(EtcdConfigurationSourceImpl.class);
  private static final String ETCD_CONFIG_URL_KEY = "etcd.config.serverUri";
  private Client client;

  private final Map<String, Object> valueCache = Maps.newConcurrentMap();
  private final List<WatchedUpdateListener> listeners = new CopyOnWriteArrayList<WatchedUpdateListener>();

  @Override
  public void addUpdateListener(WatchedUpdateListener l) {
    if (l != null) {
      listeners.add(l);
    }
  }

  @Override
  public void removeUpdateListener(WatchedUpdateListener l) {
    if (l != null){
      listeners.remove(l);
    }
  }

  @Override
  public Map<String, Object> getCurrentData() {
    return valueCache;
  }

  @Override
  public boolean isValidSource(Configuration localConfiguration) {
    return true;
  }

  @Override
  public void init(Configuration localConfiguration) {
    String endpoint = localConfiguration.getString(ETCD_CONFIG_URL_KEY);
    client = Client.builder().endpoints(endpoint).build();

    syncLocalConfig(localConfiguration);

    ByteSequence prefix = ByteSequence.from("", UTF_8);

    Watch.Listener listener = Watch.listener(response -> {
      Map<String, Object> createConfigs = new HashMap<>();
      Map<String, Object> setConfigs = new HashMap<>();
      Map<String, Object> deleteConfigs = new HashMap<>();
      for (WatchEvent event : response.getEvents()) {
        KeyValue kv = event.getKeyValue();
        String key = Optional.ofNullable(kv.getKey())
            .map(bs -> bs.toString(UTF_8))
            .orElse("");
        String value =Optional.ofNullable(kv.getValue())
            .map(bs -> bs.toString(UTF_8))
            .orElse("");

        LOGGER.info("type={}, key={}, value={}",
            event.getEventType(),key, value);
        switch (event.getEventType()){
          case PUT:
            if (key.isEmpty() && kv.getValue()==null){
              continue;
            }
            if (valueCache.containsKey(key)){
              setConfigs.put(key, value);
            } else{
              createConfigs.put(key, value);
            }
            break;
          case DELETE:
            if (!key.isEmpty() && valueCache.containsKey(key)){
              deleteConfigs.put(key,value);
            }
            break;
          case UNRECOGNIZED:
            break;
        }
        handle("create", createConfigs);
        handle("set", setConfigs);
        handle("delete", deleteConfigs);
      }
    });

    try {
      Watch watch = client.getWatchClient();
      watch.watch(prefix,
          WatchOption.newBuilder().withPrefix(prefix).build(),
          listener);
    } catch (Exception e) {
      LOGGER.error("Watching Error {}", e);
    }
    finally {
      //watcher.close();
    }
  }

  private void syncLocalConfig(Configuration localConfig){
    Iterator<String> keys = localConfig.getKeys();
    while( keys.hasNext()) {
      String key = keys.next();
      String prop = localConfig.getProperty(key).toString();
      KV kvClient = client.getKVClient();
      try {
        kvClient.put(ByteSequence.from(key, UTF_8), ByteSequence.from(prop, UTF_8)).get();
      }catch (Exception e){
        LOGGER.info("Exception putting key {}, e: {}", key, e);
      }
    }
  }

  private void updateConfiguration(WatchedUpdateResult result) {
    for (WatchedUpdateListener l : listeners) {
      try {
        l.updateConfiguration(result);
      } catch (Throwable ex) {
        LOGGER.error("Error in invoking WatchedUpdateListener", ex);
      }
    }
  }

  public void handle(String action, Map<String, Object> parseConfigs) {
    if (parseConfigs == null || parseConfigs.isEmpty()) {
      return;
    }
    Map<String, Object> configuration = ConfigMapping.getConvertedMap(parseConfigs);
    if ("create".equals(action)) {
      valueCache.putAll(configuration);
      updateConfiguration(createIncremental(ImmutableMap.<String, Object>copyOf(configuration),
          null,
          null));
    } else if ("set".equals(action)) {
      valueCache.putAll(configuration);
      updateConfiguration(createIncremental(null, ImmutableMap.<String, Object>copyOf(configuration),
          null));
    } else if ("delete".equals(action)) {
      for (String itemKey : configuration.keySet()) {
        valueCache.remove(itemKey);
      }
      updateConfiguration(createIncremental(null,
          null,
          ImmutableMap.<String, Object>copyOf(configuration)));
    } else {
      LOGGER.error("action: {} is invalid.", action);
      return;
    }
    LOGGER.warn("Config value cache changed: action:{}; item:{}", action, configuration.keySet());
  }
}
