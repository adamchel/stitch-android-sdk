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

package com.mongodb.stitch.core.services.mongodb.remote.sync.internal;

import static com.mongodb.stitch.core.internal.common.Assertions.keyPresent;
import static com.mongodb.stitch.core.services.mongodb.remote.sync.internal.CoreDocumentSynchronizationConfig.getDocFilter;
import static com.mongodb.stitch.core.services.mongodb.remote.sync.internal.CoreDocumentSynchronizationConfig.getNamespaceForStorage;
import static com.mongodb.stitch.core.services.mongodb.remote.sync.internal.CoreDocumentSynchronizationConfig.getNamespaceFromStorage;

import com.mongodb.Block;
import com.mongodb.MongoNamespace;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.stitch.core.services.mongodb.remote.sync.ChangeEventListener;
import com.mongodb.stitch.core.services.mongodb.remote.sync.ConflictHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;

import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonReader;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

class NamespaceSynchronizationConfig
    implements Iterable<CoreDocumentSynchronizationConfig> {

  private final MongoCollection<NamespaceSynchronizationConfig> namespacesColl;
  private final MongoCollection<CoreDocumentSynchronizationConfig> docsColl;
  private final MongoNamespace namespace;
  private final Map<BsonValue, CoreDocumentSynchronizationConfig> syncedDocuments;
  private final ReadWriteLock nsLock;

  private NamespaceListenerConfig namespaceListenerConfig;
  private ConflictHandler conflictHandler;
  private Codec documentCodec;

  NamespaceSynchronizationConfig(
      final MongoCollection<NamespaceSynchronizationConfig> namespacesColl,
      final MongoCollection<CoreDocumentSynchronizationConfig> docsColl,
      final MongoNamespace namespace
  ) {
    this.namespacesColl = namespacesColl;
    this.docsColl = docsColl;
    this.namespace = namespace;
    this.syncedDocuments = new HashMap<>();
    this.nsLock = new ReentrantReadWriteLock();

    // Fill from db
    final BsonDocument docsFilter = new BsonDocument();
    docsFilter.put(
        CoreDocumentSynchronizationConfig.ConfigCodec.Fields.NAMESPACE_FIELD,
        CoreDocumentSynchronizationConfig.getNamespaceForStorage(namespace));
    docsColl.find(docsFilter).forEach(new Block<CoreDocumentSynchronizationConfig>() {
      @Override
      public void apply(
          @Nonnull final CoreDocumentSynchronizationConfig docConfig
      ) {
        syncedDocuments.put(docConfig.getDocumentId(), new CoreDocumentSynchronizationConfig(
            docsColl,
            docConfig));
      }
    });
  }

  NamespaceSynchronizationConfig(
      final MongoCollection<NamespaceSynchronizationConfig> namespacesColl,
      final MongoCollection<CoreDocumentSynchronizationConfig> docsColl,
      final NamespaceSynchronizationConfig config
  ) {
    this.namespacesColl = namespacesColl;
    this.docsColl = docsColl;
    this.namespace = config.namespace;
    this.syncedDocuments = new HashMap<>();
    this.nsLock = config.nsLock;

    // Fill from db
    final BsonDocument docsFilter = new BsonDocument();
    docsFilter.put(
        CoreDocumentSynchronizationConfig.ConfigCodec.Fields.NAMESPACE_FIELD,
        CoreDocumentSynchronizationConfig.getNamespaceForStorage(namespace));
    docsColl.find(docsFilter).forEach(new Block<CoreDocumentSynchronizationConfig>() {
      @Override
      public void apply(
          @Nonnull final CoreDocumentSynchronizationConfig docConfig
      ) {
        syncedDocuments.put(docConfig.getDocumentId(), new CoreDocumentSynchronizationConfig(
            docsColl,
            docConfig));
      }
    });
  }

  private NamespaceSynchronizationConfig(
      final MongoNamespace namespace
  ) {
    this.namespace = namespace;
    this.namespacesColl = null;
    this.docsColl = null;
    this.syncedDocuments = null;
    this.nsLock = new ReentrantReadWriteLock();
  }

  <T> void configure(final ConflictHandler<T> conflictHandler,
                     final ChangeEventListener<T> changeEventListener,
                     final Codec<T> codec) {
    nsLock.writeLock().lock();
    try {
      this.conflictHandler = conflictHandler;
      this.namespaceListenerConfig = new NamespaceListenerConfig(changeEventListener, codec);
      this.documentCodec = codec;
    } finally {
      nsLock.writeLock().unlock();
    }
  }

  boolean isConfigured() {
    nsLock.readLock().lock();
    try {
      return this.conflictHandler != null;
    } finally {
      nsLock.readLock().unlock();
    }
  }

  static BsonDocument getNsFilter(
      final MongoNamespace namespace
  ) {
    final BsonDocument filter = new BsonDocument();
    filter.put(ConfigCodec.Fields.NAMESPACE_FIELD, getNamespaceForStorage(namespace));
    return filter;
  }

  public MongoNamespace getNamespace() {
    return namespace;
  }

  NamespaceListenerConfig getNamespaceListenerConfig() {
    return namespaceListenerConfig;
  }

  public CoreDocumentSynchronizationConfig getSynchronizedDocument(final BsonValue documentId) {
    nsLock.readLock().lock();
    try {
      return syncedDocuments.get(documentId);
    } finally {
      nsLock.readLock().unlock();
    }
  }

  public Set<CoreDocumentSynchronizationConfig> getSynchronizedDocuments() {
    nsLock.readLock().lock();
    try {
      return new HashSet<>(syncedDocuments.values());
    } finally {
      nsLock.readLock().unlock();
    }
  }

  public Set<BsonValue> getSynchronizedDocumentIds() throws InterruptedException {
    nsLock.readLock().lockInterruptibly();
    try {
      return new HashSet<>(syncedDocuments.keySet());
    } finally {
      nsLock.readLock().unlock();
    }
  }

  Set<BsonValue> getStaleDocumentIds() {
    nsLock.readLock().lock();
    try {
      final DistinctIterable<BsonValue> staleDocIds = this.docsColl.distinct(
          CoreDocumentSynchronizationConfig.ConfigCodec.Fields.DOCUMENT_ID_FIELD,
          new BsonDocument(
              CoreDocumentSynchronizationConfig.ConfigCodec.Fields.IS_STALE, BsonBoolean.TRUE),
          BsonValue.class);
      return staleDocIds.into(new HashSet<>());
    } finally {
      nsLock.readLock().unlock();
    }
  }

  Codec getDocumentCodec() {
    return documentCodec;
  }

  CoreDocumentSynchronizationConfig addSynchronizedDocument(
      final MongoNamespace namespace,
      final BsonValue documentId
  ) {

    final CoreDocumentSynchronizationConfig newConfig;

    final CoreDocumentSynchronizationConfig existingConfig = getSynchronizedDocument(documentId);
    if (existingConfig == null) {
      newConfig = new CoreDocumentSynchronizationConfig(
          docsColl,
          namespace,
          documentId);
    } else {
      newConfig = new CoreDocumentSynchronizationConfig(
          docsColl,
          existingConfig);
    }

    nsLock.writeLock().lock();
    try {
      docsColl.replaceOne(
          getDocFilter(newConfig.getNamespace(), newConfig.getDocumentId()),
          newConfig,
          new ReplaceOptions().upsert(true));
      syncedDocuments.put(documentId, newConfig);
      return newConfig;
    } finally {
      nsLock.writeLock().unlock();
    }
  }

  public void removeSynchronizedDocument(final BsonValue documentId) {
    nsLock.writeLock().lock();
    try {
      docsColl.deleteOne(getDocFilter(namespace, documentId));
      syncedDocuments.remove(documentId);
    } finally {
      nsLock.writeLock().unlock();
    }
  }

  public ConflictHandler getConflictHandler() {
    nsLock.readLock().lock();
    try {
      return conflictHandler;
    } finally {
      nsLock.readLock().unlock();
    }
  }

  void setStale(final boolean stale) throws InterruptedException {
    nsLock.writeLock().lockInterruptibly();
    try {
      docsColl.updateMany(
          getNsFilter(getNamespace()),
          new BsonDocument("$set",
              new BsonDocument(
                  CoreDocumentSynchronizationConfig.ConfigCodec.Fields.IS_STALE,
                  new BsonBoolean(stale))));
    } catch (IllegalStateException e) {
      // eat this
    } finally {
      nsLock.writeLock().unlock();
    }
  }

  @Override
  @Nonnull
  public Iterator<CoreDocumentSynchronizationConfig> iterator() {
    nsLock.readLock().lock();
    try {
      return new ArrayList<>(syncedDocuments.values()).iterator();
    } finally {
      nsLock.readLock().unlock();
    }
  }

  BsonDocument toBsonDocument() {
    nsLock.readLock().lock();
    try {
      final BsonDocument asDoc = new BsonDocument();
      asDoc.put(ConfigCodec.Fields.NAMESPACE_FIELD, getNamespaceForStorage(namespace));
      asDoc.put(ConfigCodec.Fields.SCHEMA_VERSION_FIELD, new BsonInt32(2));
      return asDoc;
    } finally {
      nsLock.readLock().unlock();
    }
  }

  ReadWriteLock getLock() {
    return nsLock;
  }

  static NamespaceSynchronizationConfig fromBsonDocument(final BsonDocument document) {
    keyPresent(ConfigCodec.Fields.NAMESPACE_FIELD, document);
    keyPresent(ConfigCodec.Fields.SCHEMA_VERSION_FIELD, document);

    final int schemaVersion =
        document.getNumber(ConfigCodec.Fields.SCHEMA_VERSION_FIELD).intValue();
    if (schemaVersion != 1 && schemaVersion != 2) {
      throw new IllegalStateException(
          String.format(
              "unexpected schema version '%d' for %s",
              schemaVersion,
              CoreDocumentSynchronizationConfig.class.getSimpleName()));
    }

    final MongoNamespace namespace;

    // old schema version used namespace string instead of document
    if (schemaVersion == 1) {
      namespace =
          new MongoNamespace(document.getString(ConfigCodec.Fields.NAMESPACE_FIELD).getValue());
    } else {
      namespace =
          getNamespaceFromStorage(document.getDocument(ConfigCodec.Fields.NAMESPACE_FIELD));
    }

    return new NamespaceSynchronizationConfig(namespace);
  }

  static final ConfigCodec configCodec = new ConfigCodec();

  static final class ConfigCodec implements Codec<NamespaceSynchronizationConfig> {

    @Override
    public NamespaceSynchronizationConfig decode(
        final BsonReader reader,
        final DecoderContext decoderContext
    ) {
      final BsonDocument document = (new BsonDocumentCodec()).decode(reader, decoderContext);
      return fromBsonDocument(document);
    }

    @Override
    public void encode(
        final BsonWriter writer,
        final NamespaceSynchronizationConfig value,
        final EncoderContext encoderContext
    ) {
      new BsonDocumentCodec().encode(writer, value.toBsonDocument(), encoderContext);
    }

    @Override
    public Class<NamespaceSynchronizationConfig> getEncoderClass() {
      return NamespaceSynchronizationConfig.class;
    }

    static class Fields {
      static final String NAMESPACE_FIELD = "namespace";
      static final String SCHEMA_VERSION_FIELD = "schema_version";
    }
  }
}
