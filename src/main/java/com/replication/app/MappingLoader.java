package com.replication.app;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;

import jakarta.annotation.PostConstruct;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Scans a configurable directory for *.yml mapping files at startup.
 * Each file can define its own target-schemas and envelope configuration.
 * All schemas from all files are merged and loaded into ReplicationProperties.
 *
 * This allows new domains (topics + table mappings) to be added by simply
 * dropping a new .yml file into the mappings directory — no changes to
 * application.yml or any Java code required.
 *
 * Configured via: app.mapping-dir (path to the folder containing *.yml files)
 */
@Component
public class MappingLoader {

    private static final Logger log = LogManager.getLogger(MappingLoader.class);

    private final ReplicationProperties replicationProperties;

    @Value("${app.mapping-dir}")
    private String mappingDir;

    public MappingLoader(ReplicationProperties replicationProperties) {
        this.replicationProperties = replicationProperties;
    }

    @PostConstruct
    public void load() {
        File dir = new File(mappingDir);

        if (!dir.exists() || !dir.isDirectory()) {
            log.error("Mapping directory not found: {}", dir.getAbsolutePath());
            throw new IllegalStateException("Mapping directory does not exist: " + dir.getAbsolutePath());
        }

        File[] ymlFiles = dir.listFiles((d, name) -> name.endsWith(".yml") || name.endsWith(".yaml"));

        if (ymlFiles == null || ymlFiles.length == 0) {
            log.warn("No .yml mapping files found in: {}", dir.getAbsolutePath());
            return;
        }

        List<Map<String, Object>> allSchemas = new ArrayList<>();
        ReplicationProperties.Envelope envelope = null;
        Yaml yaml = new Yaml();

        for (File file : ymlFiles) {
            log.info("Loading mapping file: {}", file.getName());
            try (FileInputStream fis = new FileInputStream(file)) {
                Map<String, Object> doc = yaml.load(fis);
                if (doc == null) {
                    log.warn("Empty mapping file skipped: {}", file.getName());
                    continue;
                }

                // Read envelope (operation-path) — first file wins if multiple define it
                if (envelope == null) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> envelopeMap = (Map<String, Object>) doc.get("envelope");
                    if (envelopeMap != null) {
                        String opPath = (String) envelopeMap.get("operation-path");
                        if (opPath != null) {
                            ReplicationProperties.Envelope e = new ReplicationProperties.Envelope();
                            e.setOperationPath(opPath);
                            envelope = e;
                        }
                    }
                }

                // Collect target-schemas from this file
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> schemas = (List<Map<String, Object>>) doc.get("target-schemas");
                if (schemas != null) {
                    allSchemas.addAll(schemas);
                    log.info("  → Loaded {} schema(s) from {}", schemas.size(), file.getName());
                }

            } catch (IOException e) {
                log.error("Failed to read mapping file: {}", file.getName(), e);
            }
        }

        // Populate ReplicationProperties with the merged result
        replicationProperties.setTargetSchemas(allSchemas);
        if (envelope != null) {
            replicationProperties.setEnvelope(envelope);
        }

        // Log summary of all topics this app will now listen to
        List<String> topics = replicationProperties.getTopics();
        log.info("Mapping loader complete — {} schema(s) loaded across {} topic(s): {}",
                allSchemas.size(), topics.size(), topics);
    }
}
