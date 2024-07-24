package com.alibaba.nacos.naming.monitor;

import com.alibaba.nacos.auth.config.AuthConfigs;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.utils.ThreadFactoryBuilder;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.alibaba.nacos.sys.utils.ApplicationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * ops monitor.
 *
 * @author eric
 */
@Component
public class OpsMonitor implements InitializingBean {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpsMonitor.class);

    @Value("${server.port}")
    private int serverPot = 8848;

    private String url;

    private ExecutorService executorService;

    /**
     * init method.
     */
    public void init() {
        // if nacos.naming.ops.monitor.enabled = true, then go on
        boolean enabled = Boolean.getBoolean("nacos.naming.ops.monitor.enabled");
        if (!enabled) {
            LOGGER.info("nacos ops monitor is disabled");
            return;
        }
        LOGGER.info("nacos ops monitor is enabled");

        url = String.format(
                "http://localhost:%s/nacos/v1/ns/instance?ip=localhost&port=52520&serviceName=test-persistent-instance-rerver&ephemeral=false", serverPot);

        // if nacos started, then go on
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().nameFormat("ops-monitor").build();
        executorService = new ThreadPoolExecutor(0, 1,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                namedThreadFactory);
        executorService.submit(() -> {
            while (!ApplicationUtils.isStarted()) {
                LOGGER.info("nacos ops monitor is waiting for nacos started...");
                try {
                    Thread.sleep(1000L);
                } catch (Throwable ignore) { }
            }
            registerPersistentInstance();
        });
    }

    private void registerPersistentInstance() {
        // register a persistent instance to nacos using v1 api, check errCode and errMsg
        RestTemplate restTemplate = new RestTemplate();
        ResponseEntity<String> response = null;
        try {
            LOGGER.info("register persistent instance to test whether raft module is ok or not");
            response = restTemplate.exchange(url, HttpMethod.POST, createAuthHeaderEntity(), String.class);
            LOGGER.info("register persistent instance ok, response status: {}, response body: {}",
                    response.getStatusCode(), response.getBody());
        } catch (Exception e) {
            if (response != null) {
                LOGGER.error("register persistent instance error, response status: {}, response body: {}",
                        response.getStatusCode(), response.getBody(), e);
            } else {
                LOGGER.error("register persistent instance error", e);
            }
            deleteRaftDir();
        } finally {
            deletePersistentInstance();
        }
    }

    private void deletePersistentInstance() {
        RestTemplate restTemplate = new RestTemplate();
        try {
            LOGGER.info("delete persistent instance to recover naming list");
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.DELETE, createAuthHeaderEntity(), String.class);
            LOGGER.info("delete persistent instance ok, response status: {}, response body: {}",
                    response.getStatusCode(), response.getBody());
        } catch (Exception ignore) {
            LOGGER.info("delete persistent instance to recover naming list error, will ignore it", ignore);
        }
    }

    private HttpEntity<Void> createAuthHeaderEntity() {
        HttpHeaders headers = new HttpHeaders();
        headers.add("User-Agent", "Nacos-Server:2.3.2");
        AuthConfigs authConfigs = ApplicationUtils.getBean(AuthConfigs.class);
        if (StringUtils.isNotBlank(authConfigs.getServerIdentityKey())) {
            headers.add(authConfigs.getServerIdentityKey(), authConfigs.getServerIdentityValue());
        }
        return new HttpEntity<>(headers);
    }

    private void deleteRaftDir() {
        String dirName = "data/protocol/raft";
        File file = new File(Paths.get(EnvUtil.getNacosHome(), dirName).toUri());
        LOGGER.info("delete raft module dir: {}", file.getAbsolutePath());
        file.delete();
        LOGGER.info("system exit");
        System.exit(100);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        init();
    }
}
