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

import com.mongodb.client.MongoClient;

public abstract class SyncMetadataMigration {

  protected final MongoClient localClient;

  public SyncMetadataMigration(final MongoClient localClient) {
    this.localClient = localClient;
  }

  /**
   * Backs up any data that is needed to restore data affected by an in-flight migration to its
   * pre-migration state.
   */
  public abstract void performBackup();

  /**
   * Performs the core logic of the migration. Implementations do not need to be idempotent,
   * because if a migration is determined to be interrupted, a backup will be restored before the
   * migration is attempted again.
   */
  public abstract void performMigration();

  /**
   * Using backup data, restores any data affected by an in-flight migration to its pre-migration
   * state. This must be implemented in an idempotent way since this method may itself be
   * interrupted.
   */
  public abstract void restoreFromBackup();

  /**
   * Cleans up any backup data used by this migration. Will only be executed after the migration
   * is successfully completed, or if the backup was interrupted, and we want to clean up the
   * backup before generating it again. Should be implemented in such a way that it will still work
   * if backup data is partially or completely missing. This is because cleanup may be interrupted.
   */
  public abstract void cleanupBackup();
}
