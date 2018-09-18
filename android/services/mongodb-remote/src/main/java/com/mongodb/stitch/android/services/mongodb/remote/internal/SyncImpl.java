package com.mongodb.stitch.android.services.mongodb.remote.internal;

import com.google.android.gms.tasks.Task;
import com.mongodb.MongoNamespace;
import com.mongodb.stitch.android.core.internal.common.TaskDispatcher;
import com.mongodb.stitch.android.services.mongodb.remote.RemoteFindIterable;
import com.mongodb.stitch.android.services.mongodb.remote.Sync;
import com.mongodb.stitch.android.services.mongodb.remote.SyncFindIterable;
import com.mongodb.stitch.core.services.mongodb.remote.RemoteDeleteResult;
import com.mongodb.stitch.core.services.mongodb.remote.RemoteInsertOneResult;
import com.mongodb.stitch.core.services.mongodb.remote.RemoteUpdateResult;
import com.mongodb.stitch.core.services.mongodb.remote.sync.ChangeEventListener;
import com.mongodb.stitch.core.services.mongodb.remote.sync.ConflictHandler;
import com.mongodb.stitch.core.services.mongodb.remote.sync.CoreSync;

import org.bson.BsonValue;
import org.bson.codecs.Codec;
import org.bson.conversions.Bson;

import java.util.Set;
import java.util.concurrent.Callable;

public class SyncImpl<DocumentT> implements Sync<DocumentT> {
  private final CoreSync<DocumentT> proxy;
  private final TaskDispatcher dispatcher;

  SyncImpl(final CoreSync<DocumentT> proxy,
           final TaskDispatcher dispatcher) {
    this.proxy = proxy;
    this.dispatcher = dispatcher;
  }

  @Override
  public void configure(final MongoNamespace namespace,
                        final ConflictHandler<DocumentT> conflictHandler,
                        final ChangeEventListener<DocumentT> changeEventListener) {
    this.proxy.configure(namespace, conflictHandler, changeEventListener);
  }

  @Override
  public void syncOne(final BsonValue id) {
    proxy.syncOne(id);
  }

  @Override
  public void syncMany(final BsonValue... ids) {
    proxy.syncMany(ids);
  }

  @Override
  public void desyncOne(final BsonValue id) {
    proxy.desyncOne(id);
  }

  @Override
  public void desyncMany(final BsonValue... ids) {
    proxy.desyncMany(ids);
  }

  @Override
  public Set<BsonValue> getSyncedIds() {
    return this.proxy.getSyncedIds();
  }

  @Override
  public SyncFindIterable<DocumentT> find() {
        return new SyncFindIterableImpl<>(proxy.find(), dispatcher);
  }

  @Override
  public SyncFindIterable<DocumentT> find(final Bson filter) {
    return new SyncFindIterableImpl<>(proxy.find(filter), dispatcher);
  }

  @Override
  public <ResultT> SyncFindIterable<ResultT> find(final Class<ResultT> resultClass) {
    return new SyncFindIterableImpl<>(proxy.find(resultClass), dispatcher);
  }

  @Override
  public <ResultT> SyncFindIterable<ResultT> find(final Bson filter,
                                                  final Class<ResultT> resultClass) {
    return new SyncFindIterableImpl<>(proxy.find(filter, resultClass), dispatcher);
  }

  @Override
  public Task<DocumentT> findOneById(final BsonValue documentId) {
    return this.dispatcher.dispatchTask(new Callable<DocumentT>() {
      @Override
      public DocumentT call() throws Exception {
        return proxy.findOneById(documentId);
      }
    });
  }

  @Override
  public <ResultT> Task<ResultT> findOneById(final BsonValue documentId, final Class<ResultT> resultClass) {
    return this.dispatcher.dispatchTask(new Callable<ResultT>() {
      @Override
      public ResultT call() throws Exception {
        return proxy.findOneById(documentId, resultClass);
      }
    });
  }

  @Override
  public Task<RemoteDeleteResult> deleteOneById(final BsonValue documentId) {
    return this.dispatcher.dispatchTask(new Callable<RemoteDeleteResult>() {
      @Override
      public RemoteDeleteResult call() throws Exception {
        return proxy.deleteOneById(documentId);
      }
    });
  }

  @Override
  public RemoteInsertOneResult insertOneAndSync(DocumentT document) {
    return this.proxy.insertOneAndSync(document);
  }

  @Override
  public Task<RemoteUpdateResult> updateOneById(final BsonValue documentId, final Bson update) {
    return this.dispatcher.dispatchTask(new Callable<RemoteUpdateResult>() {
      @Override
      public RemoteUpdateResult call() throws Exception {
        return proxy.updateOneById(documentId, update);
      }
    });
  }
}