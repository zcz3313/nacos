package com.alibaba.nacos.naming.monitor;

import com.alibaba.nacos.auth.config.AuthConfigs;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.utils.ThreadFactoryBuilder;
import com.alibaba.nacos.core.cluster.health.ModuleHealthCheckerHolder;
import com.alibaba.nacos.core.cluster.health.ReadinessResult;
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
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

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
            LOGGER.info("[ops monitor]nacos ops monitor is disabled");
            return;
        }
        LOGGER.info("[ops monitor]nacos ops monitor is enabled");

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
                LOGGER.info("[ops monitor]nacos ops monitor is waiting for nacos started...");
                try {
                    Thread.sleep(1000L);
                } catch (Throwable ignore) { }
            }
            ReadinessResult result = ModuleHealthCheckerHolder.getInstance().checkReadiness();
            LOGGER.info("[ops monitor]nacos module health result: " + result.getResultMessage());
            startProbe();
        });
    }

    private void startProbe() {
        int intervalInSeconds = 3;
        int totalRetry = Integer.getInteger("nacos.naming.ops.monitor.retry", 20);
        int retry = 0;
        boolean succeed = false;
        while (!succeed && retry <= totalRetry) {
            try {
                Thread.sleep(intervalInSeconds * 1000L);
            } catch (Exception ignore) {

            }
            retry++;
            LOGGER.info("[ops monitor]start to probe, try " + retry + "/" + totalRetry);
            try {
                registerPersistentInstance();
                succeed = true;
            } catch (HttpServerErrorException e) {
                String exMsg = e.getMessage();
                LOGGER.error("[ops monitor]register persistent instance error, ex msg: {}", exMsg, e);
                if (e.getStatusCode() == HttpStatus.INTERNAL_SERVER_ERROR
                        && exMsg != null && exMsg.contains("did not find the Leader node")) {
                    succeed = false;
                } else {
                    succeed = true;
                }
            } catch (Exception other) {
                succeed = true;
            }
        }
        LOGGER.info("probe result: " + succeed + ", try: " + retry + "/" + totalRetry);
        deletePersistentInstance();
        if (!succeed) {
            deleteRaftDirAndExit();
        }
    }

    private void registerPersistentInstance() {
        // register a persistent instance to nacos using v1 api, check errCode and errMsg
        RestTemplate restTemplate = new RestTemplate();
        LOGGER.info("[ops monitor]register persistent instance to test whether raft module is ok or not");
        ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, createAuthHeaderEntity(), String.class);
        LOGGER.info("[ops monitor]register persistent instance ok, response status: {}, response body: {}",
                response.getStatusCode(), response.getBody());
    }

    private void deletePersistentInstance() {
        RestTemplate restTemplate = new RestTemplate();
        try {
            LOGGER.info("[ops monitor]delete persistent instance to recover naming list");
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.DELETE, createAuthHeaderEntity(), String.class);
            LOGGER.info("[ops monitor]delete persistent instance ok, response status: {}, response body: {}",
                    response.getStatusCode(), response.getBody());
        } catch (Exception ignore) {
            LOGGER.info("[ops monitor]delete persistent instance to recover naming list error, will ignore it", ignore);
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

    private void deleteRaftDirAndExit() {
        String dirName = "data/protocol/raft";
        File file = new File(Paths.get(EnvUtil.getNacosHome(), dirName).toUri());
        LOGGER.info("[ops monitor]delete raft module dir: {}", file.getAbsolutePath());
        deleteDirectory(file);
        LOGGER.info("[ops monitor]system exit");
        System.exit(100);
    }

    private boolean deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] entries = directory.listFiles();
            if (entries != null) {
                for (File entry : entries) {
                    if (entry.isDirectory()) {
                        deleteDirectory(entry);
                    } else {
                        entry.delete();
                    }
                }
            }
        }

        return directory.delete();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        init();
    }
}
