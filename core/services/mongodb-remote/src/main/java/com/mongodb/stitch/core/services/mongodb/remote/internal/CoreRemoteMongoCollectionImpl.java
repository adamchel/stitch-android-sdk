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

package com.mongodb.stitch.core.services.mongodb.remote.internal;

import static com.mongodb.stitch.core.internal.common.Assertions.notNull;

import com.mongodb.MongoNamespace;
import com.mongodb.stitch.core.internal.net.NetworkMonitor;
import com.mongodb.stitch.core.internal.net.Stream;
import com.mongodb.stitch.core.services.internal.CoreStitchServiceClient;
import com.mongodb.stitch.core.services.mongodb.remote.ChangeEvent;
import com.mongodb.stitch.core.services.mongodb.remote.CompactChangeEvent;
import com.mongodb.stitch.core.services.mongodb.remote.RemoteCountOptions;
import com.mongodb.stitch.core.services.mongodb.remote.RemoteDeleteResult;
import com.mongodb.stitch.core.services.mongodb.remote.RemoteFindOneAndModifyOptions;
import com.mongodb.stitch.core.services.mongodb.remote.RemoteFindOptions;
import com.mongodb.stitch.core.services.mongodb.remote.RemoteInsertManyResult;
import com.mongodb.stitch.core.services.mongodb.remote.RemoteInsertOneResult;
import com.mongodb.stitch.core.services.mongodb.remote.RemoteUpdateOptions;
import com.mongodb.stitch.core.services.mongodb.remote.RemoteUpdateResult;
import com.mongodb.stitch.core.services.mongodb.remote.sync.CoreSync;
import com.mongodb.stitch.core.services.mongodb.remote.sync.internal.CoreSyncImpl;
import com.mongodb.stitch.core.services.mongodb.remote.sync.internal.DataSynchronizer;
import com.mongodb.stitch.core.services.mongodb.remote.sync.internal.SyncOperations;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

public class CoreRemoteMongoCollectionImpl<DocumentT>
    implements CoreRemoteMongoCollection<DocumentT> {

  private final MongoNamespace namespace;
  private final Class<DocumentT> documentClass;
  private final CoreStitchServiceClient service;
  private final Operations<DocumentT> operations;
  // Sync related fields
  private final CoreSync<DocumentT> coreSync;
  private final DataSynchronizer dataSynchronizer;
  private final NetworkMonitor networkMonitor;

  CoreRemoteMongoCollectionImpl(final MongoNamespace namespace,
                                final Class<DocumentT> documentClass,
                                final CoreStitchServiceClient service,
                                final DataSynchronizer dataSynchronizer,
                                final NetworkMonitor networkMonitor) {
    notNull("namespace", namespace);
    notNull("documentClass", documentClass);
    this.namespace = namespace;
    this.documentClass = documentClass;
    this.service = service;
    this.operations = new Operations<>(namespace, documentClass, service.getCodecRegistry());
    this.dataSynchronizer = dataSynchronizer;
    this.networkMonitor = networkMonitor;

    this.coreSync = new CoreSyncImpl<>(
      getNamespace(),
      getDocumentClass(),
      dataSynchronizer,
      service,
      new SyncOperations<>(
        getNamespace(),
        getDocumentClass(),
        dataSynchronizer,
        getCodecRegistry())
    );
  }

  /**
   * Gets the namespace of this collection.
   *
   * @return the namespace
   */
  public MongoNamespace getNamespace() {
    return namespace;
  }

  /**
   * Get the class of documents stored in this collection.
   *
   * @return the class
   */
  public Class<DocumentT> getDocumentClass() {
    return documentClass;
  }

  /**
   * Get the codec registry for the CoreRemoteMongoCollection.
   *
   * @return the {@link org.bson.codecs.configuration.CodecRegistry}
   */
  public CodecRegistry getCodecRegistry() {
    return service.getCodecRegistry();
  }

  public <NewDocumentT> CoreRemoteMongoCollection<NewDocumentT> withDocumentClass(
      final Class<NewDocumentT> clazz
  ) {
    return new CoreRemoteMongoCollectionImpl<>(
      namespace,
      clazz,
      service,
      dataSynchronizer,
      networkMonitor);
  }

  public CoreRemoteMongoCollection<DocumentT> withCodecRegistry(final CodecRegistry codecRegistry) {
    return new CoreRemoteMongoCollectionImpl<>(
      namespace,
      documentClass,
      service.withCodecRegistry(codecRegistry),
      dataSynchronizer,
      networkMonitor);
  }

  /**
   * Counts the number of documents in the collection.
   *
   * @return the number of documents in the collection
   */
  public long count() {
    return count(new BsonDocument(), new RemoteCountOptions());
  }

  /**
   * Counts the number of documents in the collection according to the given options.
   *
   * @param filter the query filter
   * @return the number of documents in the collection
   */
  public long count(final Bson filter) {
    return count(filter, new RemoteCountOptions());
  }

  /**
   * Counts the number of documents in the collection according to the given options.
   *
   * @param filter  the query filter
   * @param options the options describing the count
   * @return the number of documents in the collection
   */
  public long count(final Bson filter, final RemoteCountOptions options) {
    return operations.count(filter, options).execute(service);
  }

  /**
   * Finds a document in the collection
   *
   * @return the resulting document
   */
  public DocumentT findOne() {
    return executeFindOne(new BsonDocument(), null, documentClass);
  }

  /**
   * Finds a document in the collection.
   *
   * @param resultClass the class to decode each document into
   * @param <ResultT>   the target document type
   * @return the resulting document
   */
  public <ResultT> ResultT findOne(final Class<ResultT> resultClass) {
    return executeFindOne(new BsonDocument(), null, resultClass);
  }

  /**
   * Finds a document in the collection.
   *
   * @param filter the query filter
   * @return the resulting document
   */
  public DocumentT findOne(final Bson filter) {
    return executeFindOne(filter, null, documentClass);
  }

  /**
   * Finds a document in the collection.
   *
   * @param filter      the query filter
   * @param resultClass the class to decode each document into
   * @param <ResultT>   the target document type of the iterable.
   * @return the resulting document
   */
  public <ResultT> ResultT findOne(final Bson filter, final Class<ResultT> resultClass) {
    return executeFindOne(filter, null, resultClass);
  }

  /**
   * Finds a document in the collection.
   *
   * @param filter the query filter
   * @param options A RemoteFindOptions struct
   * @return the resulting document
   */
  public DocumentT findOne(final Bson filter, final RemoteFindOptions options) {
    return executeFindOne(filter, options, documentClass);
  }

  /**
   * Finds a document in the collection.
   *
   * @param filter      the query filter
   * @param options A RemoteFindOptions struct
   * @param resultClass the class to decode each document into
   * @param <ResultT>   the target document type of the iterable.
   * @return the resulting document
   */
  public <ResultT> ResultT findOne(
          final Bson filter,
          final RemoteFindOptions options,
          final Class<ResultT> resultClass) {
    return executeFindOne(filter, options, resultClass);
  }

  public <ResultT> ResultT executeFindOne(
          final Bson filter,
          final RemoteFindOptions options,
          final Class<ResultT> resultClass) {
    return operations.findOne(filter, options, resultClass).execute(service);
  }

  /**
   * Finds all documents in the collection.
   *
   * @return the find iterable interface
   */
  public CoreRemoteFindIterable<DocumentT> find() {
    return find(new BsonDocument(), documentClass);
  }

  /**
   * Finds all documents in the collection.
   *
   * @param resultClass the class to decode each document into
   * @param <ResultT>   the target document type of the iterable.
   * @return the find iterable interface
   */
  public <ResultT> CoreRemoteFindIterable<ResultT> find(final Class<ResultT> resultClass) {
    return find(new BsonDocument(), resultClass);
  }

  /**
   * Finds all documents in the collection.
   *
   * @param filter the query filter
   * @return the find iterable interface
   */
  public CoreRemoteFindIterable<DocumentT> find(final Bson filter) {
    return find(filter, documentClass);
  }

  /**
   * Finds all documents in the collection.
   *
   * @param filter      the query filter
   * @param resultClass the class to decode each document into
   * @param <ResultT>   the target document type of the iterable.
   * @return the find iterable interface
   */
  public <ResultT> CoreRemoteFindIterable<ResultT> find(
      final Bson filter,
      final Class<ResultT> resultClass
  ) {
    return createFindIterable(filter, resultClass);
  }

  private <ResultT> CoreRemoteFindIterable<ResultT> createFindIterable(
      final Bson filter,
      final Class<ResultT> resultClass
  ) {
    return new CoreRemoteFindIterableImpl<>(
        filter,
        resultClass,
        service,
        operations);
  }

  /**
   * Aggregates documents according to the specified aggregation pipeline.
   *
   * @param pipeline the aggregation pipeline
   * @return an iterable containing the result of the aggregation operation
   */
  public CoreRemoteAggregateIterable<DocumentT> aggregate(final List<? extends Bson> pipeline) {
    return aggregate(pipeline, this.documentClass);
  }

  /**
   * Aggregates documents according to the specified aggregation pipeline.
   *
   * @param pipeline    the aggregation pipeline
   * @param resultClass the class to decode each document into
   * @param <ResultT>   the target document type of the iterable.
   * @return an iterable containing the result of the aggregation operation
   */
  public <ResultT> CoreRemoteAggregateIterable<ResultT> aggregate(
      final List<? extends Bson> pipeline,
      final Class<ResultT> resultClass
  ) {
    return new CoreRemoteAggregateIterableImpl<>(
        pipeline,
        resultClass,
        service,
        operations);
  }

  /**
   * Inserts the provided document. If the document is missing an identifier, the client should
   * generate one.
   *
   * @param document the document to insert
   * @return the result of the insert one operation
   */
  public RemoteInsertOneResult insertOne(final DocumentT document) {
    return executeInsertOne(document);
  }

  private RemoteInsertOneResult executeInsertOne(final DocumentT document) {
    return operations.insertOne(document).execute(service);
  }

  /**
   * Inserts one or more documents.
   *
   * @param documents the documents to insert
   * @return the result of the insert many operation
   */
  public RemoteInsertManyResult insertMany(final List<? extends DocumentT> documents) {
    return executeInsertMany(documents);
  }

  private RemoteInsertManyResult executeInsertMany(final List<? extends DocumentT> documents) {
    return operations.insertMany(documents).execute(service);
  }

  /**
   * Removes at most one document from the collection that matches the given filter.
   * If no documents match, the collection is not modified.
   *
   * @param filter the query filter to apply the the delete operation
   * @return the result of the remove one operation
   */
  public RemoteDeleteResult deleteOne(final Bson filter) {
    return executeDelete(filter, false);
  }

  /**
   * Removes all documents from the collection that match the given query filter.
   * If no documents match, the collection is not modified.
   *
   * @param filter the query filter to apply the the delete operation
   * @return the result of the remove many operation
   */
  public RemoteDeleteResult deleteMany(final Bson filter) {
    return executeDelete(filter, true);
  }

  private RemoteDeleteResult executeDelete(
      final Bson filter,
      final boolean multi
  ) {
    return multi ? operations.deleteMany(filter).execute(service)
        : operations.deleteOne(filter).execute(service);
  }

  /**
   * Update a single document in the collection according to the specified arguments.
   *
   * @param filter a document describing the query filter, which may not be null.
   * @param update a document describing the update, which may not be null. The update to apply
   *              must include only update operators.
   * @return the result of the update one operation
   */
  public RemoteUpdateResult updateOne(final Bson filter, final Bson update) {
    return updateOne(filter, update, new RemoteUpdateOptions());
  }

  /**
   * Update a single document in the collection according to the specified arguments.
   *
   * @param filter        a document describing the query filter, which may not be null.
   * @param update        a document describing the update, which may not be null. The update to
   *                     apply must include only update operators.
   * @param updateOptions the options to apply to the update operation
   * @return the result of the update one operation
   */
  public RemoteUpdateResult updateOne(
      final Bson filter,
      final Bson update,
      final RemoteUpdateOptions updateOptions
  ) {
    return executeUpdate(filter, update, updateOptions, false);
  }

  /**
   * Update all documents in the collection according to the specified arguments.
   *
   * @param filter a document describing the query filter, which may not be null.
   * @param update a document describing the update, which may not be null. The update to apply
   *              must include only update operators.
   * @return the result of the update many operation
   */
  public RemoteUpdateResult updateMany(final Bson filter, final Bson update) {
    return updateMany(filter, update, new RemoteUpdateOptions());
  }

  /**
   * Update all documents in the collection according to the specified arguments.
   *
   * @param filter        a document describing the query filter, which may not be null.
   * @param update        a document describing the update, which may not be null. The update to
   *                     apply must include only update operators.
   * @param updateOptions the options to apply to the update operation
   * @return the result of the update many operation
   */
  public RemoteUpdateResult updateMany(
      final Bson filter,
      final Bson update,
      final RemoteUpdateOptions updateOptions
  ) {
    return executeUpdate(filter, update, updateOptions, true);
  }

  private RemoteUpdateResult executeUpdate(
      final Bson filter,
      final Bson update,
      final RemoteUpdateOptions updateOptions,
      final boolean multi
  ) {
    return multi ? operations.updateMany(filter, update, updateOptions).execute(service)
        : operations.updateOne(filter, update, updateOptions).execute(service);
  }

  /**
   * Finds a document in the collection and performs the given update.
   *
   * @param filter the query filter
   * @param update the update document
   * @return the resulting document
   */
  public DocumentT findOneAndUpdate(final Bson filter, final Bson update) {
    return operations.findOneAndModify(
            "findOneAndUpdate",
            filter,
            update,
            new RemoteFindOneAndModifyOptions(),
            documentClass).execute(service);
  }

  /**
   * Finds a document in the collection and performs the given update.
   *
   * @param filter      the query filter
   * @param update      the update document
   * @param resultClass the class to decode each document into
   * @param <ResultT>   the target document type of the iterable.
   * @return the resulting document
   */
  public <ResultT> ResultT findOneAndUpdate(final Bson filter,
                                     final Bson update,
                                     final Class<ResultT> resultClass) {
    return operations.findOneAndModify(
            "findOneAndUpdate",
            filter,
            update,
            new RemoteFindOneAndModifyOptions(),
            resultClass).execute(service);

  }

  /**
   * Finds a document in the collection and performs the given update.
   *
   * @param filter the query filter
   * @param update the update document
   * @param options A RemoteFindOneAndModifyOptions struct
   * @return the resulting document
   */
  public DocumentT findOneAndUpdate(final Bson filter,
                             final Bson update,
                             final RemoteFindOneAndModifyOptions options) {
    return operations.findOneAndModify(
            "findOneAndUpdate",
            filter,
            update,
            options,
            documentClass).execute(service);
  }

  /**
   * Finds a document in the collection and performs the given update.
   *
   * @param filter      the query filter
   * @param update      the update document
   * @param options     A RemoteFindOneAndModifyOptions struct
   * @param resultClass the class to decode each document into
   * @param <ResultT>   the target document type of the iterable.
   * @return the resulting document
   */
  public <ResultT> ResultT findOneAndUpdate(
          final Bson filter,
          final Bson update,
          final RemoteFindOneAndModifyOptions options,
          final Class<ResultT> resultClass) {
    return operations.findOneAndModify(
            "findOneAndUpdate",
            filter,
            update,
            options,
            resultClass).execute(service);
  }

  /**
   * Finds a document in the collection and replaces it with the given document
   *
   * @param filter the query filter
   * @param replacement the document to replace the matched document with
   * @return the resulting document
   */
  public DocumentT findOneAndReplace(final Bson filter, final Bson replacement) {
    return operations.findOneAndModify(
            "findOneAndReplace",
            filter,
            replacement,
            new RemoteFindOneAndModifyOptions(),
            documentClass).execute(service);
  }

  /**
   * Finds a document in the collection and replaces it with the given document
   *
   * @param filter the query filter
   * @param replacement the document to replace the matched document with
   * @param resultClass the class to decode each document into
   * @param <ResultT>   the target document type of the iterable.
   * @return the resulting document
   */
  public <ResultT> ResultT findOneAndReplace(final Bson filter,
                                             final Bson replacement,
                                             final Class<ResultT> resultClass) {
    return operations.findOneAndModify(
            "findOneAndReplace",
            filter,
            replacement,
            new RemoteFindOneAndModifyOptions(),
            resultClass).execute(service);
  }

  /**
   * Finds a document in the collection and replaces it with the given document
   *
   * @param filter the query filter
   * @param replacement the document to replace the matched document with
   * @param options A RemoteFindOneAndModifyOptions struct
   * @return the resulting document
   */
  public DocumentT findOneAndReplace(final Bson filter,
                                     final Bson replacement,
                                     final RemoteFindOneAndModifyOptions options) {
    return operations.findOneAndModify(
            "findOneAndReplace",
            filter,
            replacement,
            options,
            documentClass).execute(service);
  }

  /**
   * Finds a document in the collection and replaces it with the given document
   *
   * @param filter the query filter
   * @param replacement the document to replace the matched document with
   * @param options     A RemoteFindOneAndModifyOptions struct
   * @param resultClass the class to decode each document into
   * @param <ResultT>   the target document type of the iterable.
   * @return the resulting document
   */
  public <ResultT> ResultT findOneAndReplace(
          final Bson filter,
          final Bson replacement,
          final RemoteFindOneAndModifyOptions options,
          final Class<ResultT> resultClass) {
    return operations.findOneAndModify(
            "findOneAndReplace",
            filter,
            replacement,
            options,
            resultClass).execute(service);
  }

  /**
   * Finds a document in the collection and delete it.
   *
   * @param filter the query filter
   * @return the resulting document
   */
  public DocumentT findOneAndDelete(final Bson filter) {
    return operations.findOneAndModify(
            "findOneAndDelete",
            filter,
            new BsonDocument(),
            new RemoteFindOneAndModifyOptions(),
            documentClass).execute(service);
  }

  /**
   * Finds a document in the collection and delete it.
   *
   * @param filter      the query filter
   * @param resultClass the class to decode each document into
   * @param <ResultT>   the target document type of the iterable.
   * @return the resulting document
   */
  public <ResultT> ResultT findOneAndDelete(final Bson filter,
                                            final Class<ResultT> resultClass) {
    return operations.findOneAndModify(
            "findOneAndDelete",
            filter,
            new BsonDocument(),
            new RemoteFindOneAndModifyOptions(),
            resultClass).execute(service);
  }

  /**
   * Finds a document in the collection and delete it.
   *
   * @param filter the query filter
   * @param options A RemoteFindOneAndModifyOptions struct
   * @return the resulting document
   */
  public DocumentT findOneAndDelete(final Bson filter,
                                    final RemoteFindOneAndModifyOptions options) {
    return operations.findOneAndModify(
            "findOneAndDelete",
            filter,
            new BsonDocument(),
            options,
            documentClass).execute(service);
  }

  /**
   * Finds a document in the collection and delete it.
   *
   * @param filter      the query filter
   * @param options     A RemoteFindOneAndModifyOptions struct
   * @param resultClass the class to decode each document into
   * @param <ResultT>   the target document type of the iterable.
   * @return the resulting document
   */
  public <ResultT> ResultT findOneAndDelete(
          final Bson filter,
          final RemoteFindOneAndModifyOptions options,
          final Class<ResultT> resultClass) {
    return operations.findOneAndModify(
            "findOneAndDelete",
            filter,
            new BsonDocument(),
            options,
            resultClass).execute(service);
  }

  /**
   * Watches a collection. The resulting stream will be notified of all events on this collection
   * that the active user is authorized to see based on the configured MongoDB rules.
   *
   * @return the stream of change events.
   */
  public Stream<ChangeEvent<DocumentT>> watch() throws InterruptedException, IOException {
    return operations.watch(false, documentClass).execute(service);
  }

  /**
   * Watches a collection. The provided BSON document will be used as a match expression filter on
   * the change events coming from the stream.
   *
   * See https://docs.mongodb.com/manual/reference/operator/aggregation/match/ for documentation
   * around how to define a match filter.
   *
   * Defining the match expression to filter ChangeEvents is similar to defining the match
   * expression for triggers: https://docs.mongodb.com/stitch/triggers/database-triggers/
   *
   * @param matchFilter the $match filter to apply to incoming change events
   * @return the stream of change events.
   */
  public Stream<ChangeEvent<DocumentT>> watchWithFilter(final BsonDocument matchFilter)
      throws InterruptedException, IOException {
    return operations.watch(matchFilter, false, documentClass).execute(service);
  }


  /**
   * Watches specified IDs in a collection.  This convenience overload supports the use case
   * of non-{@link BsonValue} instances of {@link ObjectId} by wrapping them in
   * {@link BsonObjectId} instances for the user.
   * @param ids unique object identifiers of the IDs to watch.
   * @return the stream of change events.
   */
  @Override
  public Stream<ChangeEvent<DocumentT>> watch(final ObjectId... ids)
      throws InterruptedException, IOException {
    final BsonValue[] transformedIds = new BsonValue[ids.length];

    for (int i = 0; i < ids.length; i++) {
      transformedIds[i] = new BsonObjectId(ids[i]);
    }

    return watch(transformedIds);
  }

  /**
   * Watches specified IDs in a collection.
   * @param ids the ids to watch.
   * @return the stream of change events.
   */
  @Override
  @SuppressWarnings("unchecked")
  public Stream<ChangeEvent<DocumentT>> watch(final BsonValue... ids)
      throws InterruptedException, IOException {
    return operations.watch(
        new HashSet<>(Arrays.asList(ids)),
        false,
        documentClass
    ).execute(service);
  }

  /**
   * Watches specified IDs in a collection. Requests a stream where the full document of update
   * events, and several other unnecessary fields are omitted from the change event objects
   * returned by the server. This can save on network usage when watching large documents
   *
   * This convenience overload supports the use case
   * of non-{@link BsonValue} instances of {@link ObjectId} by wrapping them in
   * {@link BsonObjectId} instances for the user.
   *
   * @param ids unique object identifiers of the IDs to watch.
   * @return the stream of change events.
   */
  @Override
  public Stream<CompactChangeEvent<DocumentT>> watchCompact(final ObjectId... ids)
      throws InterruptedException, IOException {
    final BsonValue[] transformedIds = new BsonValue[ids.length];

    for (int i = 0; i < ids.length; i++) {
      transformedIds[i] = new BsonObjectId(ids[i]);
    }

    return watchCompact(transformedIds);
  }

  /**
   * Watches specified IDs in a collection. Requests a stream where the full document of update
   * events, and several other unnecessary fields are omitted from the change event objects
   * returned by the server. This can save on network usage when watching large documents
   *
   * @param ids the ids to watch.
   * @return the stream of change events.
   */
  @Override
  @SuppressWarnings("unchecked")
  public Stream<CompactChangeEvent<DocumentT>> watchCompact(final BsonValue... ids)
      throws InterruptedException, IOException {
    return operations.watch(
        new HashSet<>(Arrays.asList(ids)),
        true,
        documentClass
    ).execute(service);
  }

  @Override
  public CoreSync<DocumentT> sync() {
    return this.coreSync;
  }
}
