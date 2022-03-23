/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.store.fixedwidth;

import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileReaderFactory;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader.EarlyEofException;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileScanLifecycleBuilder;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin.ScanFrameworkVersion;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;

import org.apache.hadoop.conf.Configuration;


public class FixedWidthFormatPlugin extends EasyFormatPlugin<FixedWidthFormatConfig> {

  protected static final String DEFAULT_NAME = "fixedwidth";

  /**
   * FixedWidthReaderFactory Inner Class
   */
  private static class FixedWidthReaderFactory extends FileReaderFactory {

    private final FixedWidthFormatConfig config;
    private final int maxRecords;

    /**
     * FixedWidthReaderFactory Constructor
     * @param config Config Object
     * @param maxRecords Maximum Records Reader will handle
     */
    public FixedWidthReaderFactory(FixedWidthFormatConfig config, int maxRecords) {
      this.config = config;
      this.maxRecords = maxRecords;
      System.out.println("FixWidthReaderFactory instantiated");
    }

    @Override
    public ManagedReader newReader(FileSchemaNegotiator negotiator) throws EarlyEofException {
      System.out.println("newReader() is called.");
      return new FixedWidthBatchReader(negotiator, this.config, this.maxRecords);
    }
  }

  /**
   * FixedWidthFormatPlugin Constructor
   * @param name
   * @param context
   * @param fsConf
   * @param storageConfig
   * @param formatConfig
   */
  public FixedWidthFormatPlugin(String name,
                                DrillbitContext context,
                                Configuration fsConf,
                                StoragePluginConfig storageConfig,
                                FixedWidthFormatConfig formatConfig) {
    super(name, easyConfig(fsConf, formatConfig), context, storageConfig, formatConfig);
    System.out.println("Format Plugin created");
  }

  private static EasyFormatConfig easyConfig(Configuration fsConf, FixedWidthFormatConfig pluginConfig) {
    return EasyFormatConfig.builder()
      .readable(true)
      .writable(false)
      .blockSplittable(false) // Change to true
      .compressible(true)
      .supportsProjectPushdown(true)
      .extensions(pluginConfig.getExtensions())
      .fsConf(fsConf)
      .defaultName(DEFAULT_NAME)
//      .useEnhancedScan(true)
      .scanVersion(ScanFrameworkVersion.EVF_V2)
      .supportsLimitPushdown(true)
      .build();
  }

  @Override
  protected void configureScan(FileScanLifecycleBuilder builder, EasySubScan scan) {
    System.out.println("configureScan() called");
    builder.nullType(Types.optional(TypeProtos.MinorType.VARCHAR));
    builder.readerFactory(new FixedWidthReaderFactory(formatConfig, scan.getMaxRecords()));
  }

}
