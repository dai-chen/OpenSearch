/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;

/**
 * Loads the Substrait extension catalog used by {@link DataFusionFragmentConvertor}.
 *
 * <p>The catalog is the union of:
 * <ol>
 *   <li>{@link DefaultExtensionCatalog#DEFAULT_COLLECTION} — the eleven standard YAMLs
 *       bundled in {@code io.substrait:core:0.67.0} (arithmetic, datetime, boolean, comparison,
 *       aggregate_*, rounding_*, string, logarithmic).</li>
 *   <li>Every OpenSearch-specific YAML under {@code resources/extensions/functions_opensearch_*.yaml}
 *       on the plugin classpath. These declare PPL built-in functions that aren't in the
 *       standard catalog — e.g. {@code last_day} — so they must have matching
 *       {@code ScalarUDF}s registered on the native DataFusion SessionContext
 *       (see {@code register_opensearch_udfs} on the Rust side).</li>
 * </ol>
 *
 * <p>Callers MUST invoke the loader with this class's classloader set as the thread's
 * context classloader (TCCL). The isthmus YAML loader uses the TCCL to resolve inter-YAML
 * references.
 *
 * <p>To add a new custom function:
 * <ol>
 *   <li>Drop a YAML at {@code src/main/resources/extensions/functions_opensearch_&lt;topic&gt;.yaml}</li>
 *   <li>Add its filename to {@link #CUSTOM_YAML_RESOURCES}</li>
 *   <li>Register a Calcite operator → Substrait name mapping in
 *       {@link DataFusionFragmentConvertor#createVisitor}</li>
 *   <li>Implement a matching {@code ScalarUDF} in {@code sandbox/libs/dataformat-native/rust}
 *       and register it on every SessionContext</li>
 * </ol>
 */
final class SubstraitExtensionLoader {

    private SubstraitExtensionLoader() {}

    /**
     * Classpath resource paths for custom YAMLs to merge on top of the standard catalog.
     * Each entry must resolve against this class's classloader.
     */
    private static final String[] CUSTOM_YAML_RESOURCES = new String[] {
        "/extensions/functions_opensearch_datetime.yaml"
    };

    /** Substrait extension URIs — must match what's emitted in the Substrait plan. */
    static final String URI_OPENSEARCH_DATETIME = "extension:opensearch:functions_opensearch_datetime";

    /**
     * Returns the merged extension collection. Throws {@link UncheckedIOException} if a custom
     * YAML resource is missing or malformed — missing resources indicate a packaging bug and
     * should fail fast rather than silently downgrade to the default catalog.
     */
    static SimpleExtension.ExtensionCollection load() {
        SimpleExtension.ExtensionCollection collection = DefaultExtensionCatalog.DEFAULT_COLLECTION;
        for (String resource : CUSTOM_YAML_RESOURCES) {
            String uri = resourceUri(resource);
            try (InputStream in = SubstraitExtensionLoader.class.getResourceAsStream(resource)) {
                if (in == null) {
                    throw new IOException("Substrait extension resource not found on classpath: " + resource);
                }
                collection = collection.merge(SimpleExtension.load(uri, in));
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to load Substrait extension " + resource, e);
            }
        }
        return collection;
    }

    /** Maps a classpath path like {@code /extensions/functions_opensearch_datetime.yaml} to its URI. */
    private static String resourceUri(String resource) {
        if (resource.endsWith("functions_opensearch_datetime.yaml")) {
            return URI_OPENSEARCH_DATETIME;
        }
        throw new IllegalArgumentException("No URI mapping for resource: " + resource);
    }
}
