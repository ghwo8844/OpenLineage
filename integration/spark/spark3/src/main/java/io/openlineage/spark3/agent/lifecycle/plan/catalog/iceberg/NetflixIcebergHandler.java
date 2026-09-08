/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import static io.openlineage.spark.agent.util.PathUtils.GLUE_TABLE_PREFIX;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import io.openlineage.client.utils.filesystem.FilesystemDatasetUtils;
import io.openlineage.spark.agent.util.AwsUtils;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.agent.util.SparkConfUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogHandler;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.Path;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nullable;

@Slf4j
public class NetflixIcebergHandler implements CatalogHandler {

    private final OpenLineageContext context;

    private static final String TYPE = "type";
    private static final String CATALOG_IMPL = "catalog-impl";

    // Reinstantiate the org.apache.icebergCatalogProperties vars to avoid unnecessary imports
    private static final String WAREHOUSE_LOCATION = "warehouse";
    private static final String URI = "uri";
    private static final String SPARK_CACHED_TABLE_CATALOG =
            "org.apache.iceberg.spark.SparkCachedTableCatalog";

    public NetflixIcebergHandler(OpenLineageContext context) {
        this.context = context;
    }

    public boolean hasClasses() {
        try {
            NetflixIcebergHandler.class
                    .getClassLoader()
                    .loadClass("org.apache.iceberg.catalog.Catalog");
            return true;
        } catch (Exception e) {
            log.debug("The iceberg catalog is not present");
        }
        return false;
    }

    public boolean isClass(TableCatalog tableCatalog) {
        return tableCatalog.getClass().getName().contains("NetflixSparkCatalog")
                || isSparkCachedTableCatalog(tableCatalog);
    }

    private static boolean isSparkCachedTableCatalog(TableCatalog tableCatalog) {
        return SPARK_CACHED_TABLE_CATALOG.equals(tableCatalog.getClass().getName());
    }

    @Override
    public Optional<CatalogWithAdditionalFacets> getCatalogDatasetFacet(
            TableCatalog tableCatalog, Map<String, String> properties) {
        Optional<Map<String, String>> catalogConf =
                context.getSparkSession()
                        .map(SparkSession::conf)
                        .map(conf -> conf.getAll())
                        .map(ScalaConversionUtils::fromMap)
                        .map(map -> getCatalogProperties(map, tableCatalog.name()));

        if (!catalogConf.isPresent()) {
            return Optional.empty();
        }
        Map<String, String> conf = catalogConf.get();
        String catalogType = getCatalogType(conf);

        OpenLineage.CatalogDatasetFacetBuilder builder =
                context.getOpenLineage()
                        .newCatalogDatasetFacetBuilder()
                        .name(tableCatalog.name())
                        .framework("iceberg")
                        .type(catalogType)
                        .source("spark");

        String warehouseLocation = conf.get(WAREHOUSE_LOCATION);
        if (warehouseLocation != null && !warehouseLocation.trim().isEmpty()) {
            builder.warehouseUri(warehouseLocation);
        }

        String catalogUri = conf.get(URI);
        if (catalogUri != null && !catalogUri.trim().isEmpty()) {
            builder.metadataUri(catalogUri);
        }

        return Optional.of(CatalogWithAdditionalFacets.of(builder.build()));
    }

    public DatasetIdentifier getDatasetIdentifier(
            SparkSession session,
            TableCatalog tableCatalog,
            Identifier identifier,
            Map<String, String> properties) {
        String catalogName = tableCatalog.name();
        Map<String, String> sparkRuntimeConfig =
                ScalaConversionUtils.fromMap(session.conf().getAll());
        Map<String, String> catalogConf = getCatalogProperties(sparkRuntimeConfig, catalogName);
        String catalogType = getCatalogType(catalogConf);
        String warehouseLocation = catalogConf.get(WAREHOUSE_LOCATION);

        Identifier parsedIdentifier = parseNamespace(identifier, catalogName);

        TableInfo tableInfo =
                loadTableInfo(tableCatalog, identifier, parsedIdentifier, catalogName);
        String tableName =
                tableInfo
                        .getCatalogQualifiedLogicalName(catalogName)
                        .orElseGet(() -> getCatalogQualifiedTableName(catalogName, identifier));
        DatasetIdentifier logicalIdentifier =
                getSymlinkIdentifier(session, catalogType, catalogName, catalogConf, tableName);

        Optional<Path> tableLocation =
                tableInfo
                        .location
                        .filter(location -> !location.trim().isEmpty())
                        .map(Path::new);
        if (!tableLocation.isPresent() && !isNullOrBlank(warehouseLocation)) {
            tableLocation =
                    Optional.of(
                            reconstructDefaultLocation(
                                    new Path(warehouseLocation), parsedIdentifier));
        }
        if (!tableLocation.isPresent()) {
            log.warn(
                    "No physical location found for {}; using its logical catalog identifier",
                    parsedIdentifier);
            return logicalIdentifier;
        }

        DatasetIdentifier physicalIdentifier = PathUtils.fromPath(tableLocation.get());
        return physicalIdentifier.withSymlink(
                logicalIdentifier.getName(),
                logicalIdentifier.getNamespace(),
                SymlinkType.TABLE);
    }

    /**
     * Returns the catalog-qualified logical name represented by Spark's catalog-relative
     * identifier. This applies equally to regular tables and Iceberg metadata tables, whose final
     * name may be a metadata type such as {@code snapshots}.
     */
    static String getCatalogQualifiedTableName(String catalogName, Identifier identifier) {
        String identifierName = identifier.name().replaceAll("^`+|`+$", "");
        String[] namespace = identifier.namespace();
        if (namespace.length == 0) {
            return catalogName + "." + identifierName;
        }
        return catalogName + "." + String.join(".", namespace) + "." + identifierName;
    }

    private TableInfo loadTableInfo(
            TableCatalog tableCatalog,
            Identifier originalIdentifier,
            Identifier parsedIdentifier,
            String catalogName) {
        if (isSparkCachedTableCatalog(tableCatalog)) {
            return loadCachedTableInfo(tableCatalog, originalIdentifier);
        }
        return loadNetflixTableInfo(
                tableCatalog, originalIdentifier, parsedIdentifier, catalogName);
    }

    private TableInfo loadNetflixTableInfo(
            TableCatalog tableCatalog,
            Identifier originalIdentifier,
            Identifier parsedIdentifier,
            String catalogName) {
        // NetflixSparkCatalog path: the underlying Iceberg catalog expects the catalog-qualified
        // identifier.
        try {
            Class<?> tableIdentifierClass =
                    Class.forName("org.apache.iceberg.catalog.TableIdentifier");
            Object tableIdentifier =
                    tableIdentifierClass
                            .getMethod("parse", String.class)
                            .invoke(null, parsedIdentifier.toString());

            Object icebergCatalog =
                    tableCatalog.getClass().getMethod("icebergCatalog").invoke(tableCatalog);
            Object table =
                    icebergCatalog
                            .getClass()
                            .getMethod("loadTable", tableIdentifierClass)
                            .invoke(icebergCatalog, tableIdentifier);
            String location = (String) table.getClass().getMethod("location").invoke(table);
            String name = (String) table.getClass().getMethod("name").invoke(table);
            return TableInfo.withVerifiedCatalogQualification(location, name, catalogName);
        } catch (Exception e) {
            log.debug(
                    "Could not load table info through the Netflix Iceberg catalog: {}",
                    parsedIdentifier,
                    e);
        }

        // A Spark API fallback may still recover the authoritative catalog-relative name and
        // location. Name provenance is retained so catalog qualification happens before emission.
        try {
            Object sparkTable = tableCatalog.loadTable(originalIdentifier);
            Object icebergTable = sparkTable.getClass().getMethod("table").invoke(sparkTable);
            String location =
                    (String) icebergTable.getClass().getMethod("location").invoke(icebergTable);
            String name = (String) icebergTable.getClass().getMethod("name").invoke(icebergTable);
            return TableInfo.withVerifiedCatalogQualification(location, name, catalogName);
        } catch (Exception e) {
            log.debug(
                    "Could not load table info through the Spark catalog: {}",
                    originalIdentifier,
                    e);
        }

        log.info("Could not load table info from catalog: {}", originalIdentifier);
        return TableInfo.EMPTY;
    }

    private TableInfo loadCachedTableInfo(
            TableCatalog tableCatalog, Identifier originalIdentifier) {
        // The inbound Identifier is the cache UUID alias, so load it unchanged. Table.name() is the
        // recovered original <catalog>.<db>.<table> used for the symlink.
        try {
            Object sparkTable = tableCatalog.loadTable(originalIdentifier);
            Object icebergTable = sparkTable.getClass().getMethod("table").invoke(sparkTable);
            String location =
                    (String) icebergTable.getClass().getMethod("location").invoke(icebergTable);
            String name = (String) icebergTable.getClass().getMethod("name").invoke(icebergTable);
            return TableInfo.withCatalogQualifiedLogicalName(location, name);
        } catch (Exception e) {
            log.debug("Could not load cached table info: {}", originalIdentifier, e);
            log.info("Could not load table info from catalog: {}", originalIdentifier);
            return TableInfo.EMPTY;
        }
    }

    private static boolean isNullOrBlank(@Nullable String value) {
        return value == null || value.trim().isEmpty();
    }

    private enum LogicalNameScope {
        CATALOG_RELATIVE,
        CATALOG_QUALIFIED
    }

    private static final class TableInfo {
        static final TableInfo EMPTY = new TableInfo(null, null, null);

        final Optional<String> location;
        final Optional<String> logicalName;
        final LogicalNameScope logicalNameScope;

        private TableInfo(
                String location, String logicalName, LogicalNameScope logicalNameScope) {
            this.location = Optional.ofNullable(location);
            this.logicalName = Optional.ofNullable(logicalName);
            this.logicalNameScope = logicalNameScope;
        }

        static TableInfo withVerifiedCatalogQualification(
                String location, String logicalName, String catalogName) {
            LogicalNameScope scope =
                    logicalName != null && logicalName.startsWith(catalogName + ".")
                            ? LogicalNameScope.CATALOG_QUALIFIED
                            : LogicalNameScope.CATALOG_RELATIVE;
            return new TableInfo(location, logicalName, scope);
        }

        static TableInfo withCatalogQualifiedLogicalName(String location, String logicalName) {
            return new TableInfo(location, logicalName, LogicalNameScope.CATALOG_QUALIFIED);
        }

        Optional<String> getCatalogQualifiedLogicalName(String catalogName) {
            return logicalName
                    .filter(name -> !name.trim().isEmpty())
                    .map(
                            name ->
                                    logicalNameScope == LogicalNameScope.CATALOG_RELATIVE
                                            ? catalogName + "." + name
                                            : name);
        }
    }

    private Identifier parseNamespace(Identifier identifier, String catalogName) {
        String[] namespace = identifier.namespace();
        String parsedName = identifier.name().replaceAll("^`+|`+$", "");

        String[] parsedNamespace = new String[namespace.length + 1];
        parsedNamespace[0] = catalogName;
        System.arraycopy(namespace, 0, parsedNamespace, 1, namespace.length);
        return Identifier.of(parsedNamespace, parsedName);
    }

    private Path reconstructDefaultLocation(Path warehouseLocation, Identifier identifier) {
        // namespace1.namespace2.table -> /warehouseLocation/namespace1/namespace2/table
        String[] namespace = identifier.namespace();
        ArrayList<String> pathComponents = new ArrayList<>(namespace.length + 1);
        pathComponents.addAll(Arrays.asList(namespace));
        pathComponents.add(identifier.name());
        return new Path(warehouseLocation, String.join(Path.SEPARATOR, pathComponents));
    }

    private DatasetIdentifier getSymlinkIdentifier(
            SparkSession session,
            String catalogType,
            String catalogName,
            Map<String, String> catalogConf,
            String tableName) {
        String catalogUri = catalogConf.get(URI);
        DatasetIdentifier value;
        if ("hive".equals(catalogType)
                || "prodhive".equals(catalogName)
                || "testhive".equals(catalogName)) {
            log.debug("Getting symlink for hive");
            value = getHiveIdentifier(session, catalogUri, tableName);
        } else if ("rest".equals(catalogType)) {
            log.debug("Getting symlink for rest");
            value = getRestIdentifier(catalogUri, tableName);
        } else if ("nessie".equals(catalogType)) {
            log.debug("Getting symlink for nessie");
            value = getNessieIdentifier(catalogUri, tableName);
        } else if ("glue".equals(catalogType)) {
            log.debug("Getting symlink for glue");
            value = getGlueIdentifier(tableName, session);
        } else {
            log.debug("Getting symlink using warehouse location and table name");
            String warehouseLocation = catalogConf.get(WAREHOUSE_LOCATION);
            value =
                    FilesystemDatasetUtils.fromLocationAndName(
                            new Path(warehouseLocation).toUri(), tableName);
        }
        return value;
    }

    private Map<String, String> getCatalogProperties(Map<String, String> conf, String catalogName) {
        String propertyPrefix = String.format("spark.sql.catalog.%s.", catalogName);
        log.debug(
                "Searching for spark properties pertaining to the catalog '{}'. The catalog"
                    + " settings are prefixed with '{}'.",
                catalogName,
                propertyPrefix);
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : conf.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(propertyPrefix)) {
                String trimmedKey = key.substring(propertyPrefix.length());
                result.put(trimmedKey, entry.getValue());
            }
        }

        String globalCatalogImpl = conf.get("spark.sql.catalogImplementation");
        if (globalCatalogImpl != null) {
            result.putIfAbsent(TYPE, globalCatalogImpl);
        }

        // Obtain global warehouse directory setting
        String globalWarehouseDir = conf.get("spark.sql.warehouse.dir");
        if (globalWarehouseDir != null) {
            result.putIfAbsent(WAREHOUSE_LOCATION, globalWarehouseDir);
        }

        return result;
    }

    @SneakyThrows
    private DatasetIdentifier getHiveIdentifier(
            SparkSession session, @Nullable String confUri, String table) {
        String metastoreUri;
        if (isNullOrBlank(confUri)) {
            metastoreUri =
                    SparkConfUtils.findHadoopConfigKey(
                                    session.sparkContext().hadoopConfiguration(),
                                    "netflix.metacat.host")
                            .orElse("netflix.metacat.host not found");
        } else {
            metastoreUri = confUri;
        }

        return new DatasetIdentifier(table, metastoreUri);
    }

    @SneakyThrows
    private DatasetIdentifier getNessieIdentifier(@Nullable String confUri, String table) {
        String uri = new URI(confUri).toString();
        return new DatasetIdentifier(table, uri);
    }

    @SneakyThrows
    private DatasetIdentifier getGlueIdentifier(String table, SparkSession sparkSession) {
        SparkContext sparkContext = sparkSession.sparkContext();
        String arn =
                AwsUtils.getGlueArn(sparkContext.getConf(), sparkContext.hadoopConfiguration())
                        .get();
        return new DatasetIdentifier(GLUE_TABLE_PREFIX + table.replace(".", "/"), arn);
    }

    @SneakyThrows
    private DatasetIdentifier getRestIdentifier(@Nullable String confUri, String table) {
        String uri = new URI(confUri).toString();
        return new DatasetIdentifier(table, uri);
    }

    public Optional<OpenLineage.StorageDatasetFacet> getStorageDatasetFacet(
            Map<String, String> properties) {
        String format = properties.getOrDefault("format", "");
        return Optional.of(
                context.getOpenLineage()
                        .newStorageDatasetFacet("iceberg", format.replace("iceberg/", "")));
    }

    public Optional<String> getDatasetVersion(
            TableCatalog tableCatalog, Identifier identifier, Map<String, String> properties) {
        try {
            Table table = tableCatalog.loadTable(identifier);
            String snapshotId = table.properties().get("current-snapshot-id");
            if (snapshotId == null) {
                return Optional.empty();
            }
            try {
                return Optional.of(Long.toString(Long.parseLong(snapshotId)));
            } catch (NumberFormatException e) {
                log.debug(
                        "Skipping dataset version for {} - invalid current snapshot ID '{}'",
                        identifier,
                        snapshotId);
            }
        } catch (NoSuchTableException e) {
            log.warn("Failed to load table from catalog: {}", identifier);
        } catch (ForbiddenException e) {
            // Table access requires a secure-view referenced-by token that OpenLineage cannot
            // provide; skip version tracking silently rather than logging a misleading warning.
            log.debug(
                    "Skipping dataset version for {} - table requires view context ({})",
                    identifier,
                    e.getMessage());
        } catch (Exception e) {
            log.info("Failed to get dataset version: {}", e.getMessage());
            log.debug("Failed to get dataset version", e);
        }
        return Optional.empty();
    }

    public String getName() {
        return "netflix";
    }

    private String getCatalogType(Map<String, String> catalogConf) {
        if (catalogConf.containsKey(TYPE)) {
            String catalogType = catalogConf.get(TYPE);
            log.debug(
                    "Found the catalog type using the 'type' property. The catalog type is '{}'",
                    catalogType);
            return catalogType;
        } else if (catalogConf.containsKey(CATALOG_IMPL)
                && catalogConf.get(CATALOG_IMPL).endsWith("GlueCatalog")) {
            log.debug(
                    "Default the catalog type to 'glue' because the catalog impl is {}",
                    catalogConf.get(CATALOG_IMPL));
            return "glue";
        } else {
            return null;
        }
    }
}
