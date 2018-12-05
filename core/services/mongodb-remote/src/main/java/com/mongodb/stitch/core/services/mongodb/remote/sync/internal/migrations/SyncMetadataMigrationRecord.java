/*
 * Copyright 2018-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.mongodb.stitch.core.services.mongodb.remote.sync.internal.migrations;

import static com.mongodb.stitch.core.internal.common.Assertions.keyPresent;

import java.util.concurrent.locks.Lock;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonReader;
import org.bson.BsonString;
import org.bson.BsonWriter;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;



public final class SyncMetadataMigrationRecord {
  private SyncMetadataMigrationStatus latestMigrationStatus;
  private Integer currentMetadataVersion;

  SyncMetadataMigrationRecord(
      final SyncMetadataMigrationStatus latestMigrationStatus,
      final int latestMigrationStartVersion
  ) {
    this.latestMigrationStatus = latestMigrationStatus;
    this.currentMetadataVersion = latestMigrationStartVersion;
  }

  public SyncMetadataMigrationStatus getLatestMigrationStatus() {
    return latestMigrationStatus;
  }

  public Integer getCurrentMetadataVersion() {
    return currentMetadataVersion;
  }

  public void setCurrentMetadataVersion(final Integer currentMetadataVersion) {
    this.currentMetadataVersion = currentMetadataVersion;
  }

  public void setLatestMigrationStatus(final SyncMetadataMigrationStatus latestMigrationStatus) {
    this.latestMigrationStatus = latestMigrationStatus;
  }

  BsonDocument toBsonDocument() {
    final BsonDocument asDoc = new BsonDocument();

    asDoc.put(
        ConfigCodec.Fields.LATEST_MIGRATION_STATUS_FIELD,
        new BsonString(latestMigrationStatus.getCode())
    );
    asDoc.put(
        ConfigCodec.Fields.CURRENT_METADATA_VERSION_FIELD,
        new BsonInt32(currentMetadataVersion)
    );

    return asDoc;
  }

  static SyncMetadataMigrationRecord fromBsonDocument(final BsonDocument document) {
    keyPresent(ConfigCodec.Fields.LATEST_MIGRATION_STATUS_FIELD, document);
    keyPresent(ConfigCodec.Fields.CURRENT_METADATA_VERSION_FIELD, document);

    final String statusCode =
        document.getString(ConfigCodec.Fields.LATEST_MIGRATION_STATUS_FIELD).getValue();
    final SyncMetadataMigrationStatus status =
        SyncMetadataMigrationStatus.fromStatusCode(statusCode);

    final int startVersion =
        document.getInt32(ConfigCodec.Fields.CURRENT_METADATA_VERSION_FIELD).getValue();

    return new SyncMetadataMigrationRecord(status, startVersion);
  }

  static final class ConfigCodec implements Codec<SyncMetadataMigrationRecord> {

    @Override
    public SyncMetadataMigrationRecord decode(
        final BsonReader reader,
        final DecoderContext decoderContext
    ) {
      final BsonDocument document = (new BsonDocumentCodec()).decode(reader, decoderContext);
      return fromBsonDocument(document);
    }

    @Override
    public void encode(
        final BsonWriter writer,
        final SyncMetadataMigrationRecord value,
        final EncoderContext encoderContext
    ) {
      new BsonDocumentCodec().encode(writer, value.toBsonDocument(), encoderContext);
    }

    @Override
    public Class<SyncMetadataMigrationRecord> getEncoderClass() {
      return SyncMetadataMigrationRecord.class;
    }

    static class Fields {
      static final String CURRENT_METADATA_VERSION_FIELD = "current_metadata_version";
      static final String LATEST_MIGRATION_STATUS_FIELD = "migration_status";
    }
  }
}
