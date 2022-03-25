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

package org.apache.rocketmq.proxy.config;

import com.alibaba.fastjson.JSON;
import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Configuration {
    private final static Logger log = LoggerFactory.getLogger(Configuration.class);
    private final AtomicReference<ProxyConfig> proxyConfigReference = new AtomicReference<>();

    public void init() throws Exception {
        String proxyConfigData = loadJsonConfig(ProxyConfig.CONFIG_FILE_NAME);
        if (null == proxyConfigData) {
            throw new RuntimeException(String.format("load configuration from file: %s error.", ProxyConfig.CONFIG_FILE_NAME));
        }

        ProxyConfig proxyConfig = JSON.parseObject(proxyConfigData, ProxyConfig.class);
        setProxyConfig(proxyConfig);
    }

    public static String loadJsonConfig(String configFileName) throws Exception {
        String filePath = new File(ConfigurationManager.getProxyHome() + File.separator + "conf", configFileName).toString();

        File file = new File(filePath);
        if (!file.exists()) {
            log.warn("the config file {} not exist", filePath);
            return null;
        }
        long fileLength = file.length();
        if (fileLength <= 0) {
            log.warn("the config file {} length is zero", filePath);
            return null;
        }

        return new String(Files.readAllBytes(file.toPath()));
    }

    public ProxyConfig getProxyConfig() {
        return proxyConfigReference.get();
    }

    public void setProxyConfig(ProxyConfig proxyConfig) {
        proxyConfigReference.set(proxyConfig);
    }
}
