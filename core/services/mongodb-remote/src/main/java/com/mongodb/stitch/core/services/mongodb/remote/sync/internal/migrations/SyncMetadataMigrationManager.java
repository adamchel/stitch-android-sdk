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
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;

import java.util.HashMap;
import java.util.Map;
//import java.util.concurrent.Semaphore;
//import java.util.concurrent.locks.Lock;

import javax.annotation.Nonnull;

import org.bson.BsonDocument;

/**
 * SyncMetadataMigrationManager takes care of migrating old synchronization config documents,
 * databases, and collections to newer schemas.
 */
class SyncMetadataMigrationManager {

  static final int CURRENT_SDK_METADATA_VERSION = 3;
  // map of target version of
  private final Map<Integer, SyncMetadataMigration> migrations;

  private static void initializeMigrations(
      final Map<Integer, SyncMetadataMigration> migrations,
      final MongoClient localClient
  ) {
    migrations.put(2, new Migration2238M1(localClient));
    migrations.put(3, new Migration2238M2(localClient));
  }

  //  private static Semaphore migrationCompleted;
  //
  //  // A lock to prevent more than one migration from occurring at the same time, and to ensure that
  //  // no other data synchronizers start running before a migration is completed.
  //  private static Lock migrationManagerLock;
  //
  //  public static boolean isMigrationSuccessful() {
  //    return migrationSuccessful;
  //  }

  /**
   * Checks whether or not we should run the migration manager. The migration manager should only
   * ever be run once for an application's life, so this is a static synchronized method that will
   * return true exactly once.
   * @return true on the first execution, false every other time
   */
  private static synchronized boolean shouldRunMigrator() {
    if (migrationManagerExecutedOnce) {
      return false;
    }

    migrationManagerExecutedOnce = true;
    return true;
  }

  private static boolean migrationManagerExecutedOnce;

  private final MongoCollection<SyncMetadataMigrationRecord> migrationRecords;

  public SyncMetadataMigrationManager(
      final MongoClient localClient
  ) {
    migrations = new HashMap<>();
    migrationRecords = localClient
        .getDatabase("sync_metadata")
        .getCollection("migrations", SyncMetadataMigrationRecord.class);

    // TODO: make this lazily happen. we only want this to be initialized if migrations need to be
    // performed
    initializeMigrations(migrations, localClient);
  }

  @Nonnull
  private SyncMetadataMigrationRecord getLatestMigrationRecord() {
    final SyncMetadataMigrationRecord migrationRecord = migrationRecords.find().first();

    // if there is no migration record, it means that no migration was ever run for this app, or
    // app data was cleared. treat this as a new migration with a start version of 1
    if (migrationRecord == null) {
      return new SyncMetadataMigrationRecord(SyncMetadataMigrationStatus.COMPLETED, 1);
    }

    return migrationRecord;
  }

  private void updateMigrationRecord(
      final SyncMetadataMigrationRecord record
  ) {
    migrationRecords.updateOne(
        new BsonDocument(),
        new BsonDocument("$set", record.toBsonDocument()),
        new UpdateOptions().upsert(true)
    );
  }

  public void run() {
    try {
      // migrationManagerLock.lock();

      final SyncMetadataMigrationRecord migrationRecord = getLatestMigrationRecord();

      // When the migration manager first starts, the current state is checked before running the
      // normal state machine logic. If the migration manager is in a state other than “completed”,
      // it means a migration was interrupted, and something may need to be done to recover the
      // metadata to a usable state.
      switch (migrationRecord.getLatestMigrationStatus()) {
        case COMPLETED:
          if (migrationRecord.getCurrentMetadataVersion() == CURRENT_SDK_METADATA_VERSION) {
            // If current_metadata_version is equal to the current metadata version for the SDK,
            // then no migrations need to be performed, this will be the case of 99.99% the time
            // when the migration manager starts.
            return;
          } else if (migrationRecord.getCurrentMetadataVersion() < CURRENT_SDK_METADATA_VERSION) {
            // If current_metadata_version is less than the current metadata version of the SDK,
            // then the next state of the machine will be “backing up”, since migrations need to be
            // performed.
            migrationRecord.setLatestMigrationStatus(SyncMetadataMigrationStatus.BACKING_UP);
            updateMigrationRecord(migrationRecord);
            break;
          } else {
            // If current_metadata_version is greater than the current metadata version of the SDK,
            // then the migration manager will throw an IllegalStateException telling the developer
            // to upgrade their SDK or clear app data.
            throw new IllegalStateException(
                "MongoDB Stitch Sync metadata is using an unsupported version. may be fixed by "
                    + "upgrading to latest version of Stitch SDK, or clearing app data");
          }
        case BACKING_UP:
          // This means the app was interrupted while data was being backed up. This will set the
          // state of the machine to “cleaning_interrupted_backup” so that the backup can be tried
          // again.
          migrationRecord.setLatestMigrationStatus(
              SyncMetadataMigrationStatus.CLEANING_INTERRUPTED_BACKUP
          );
          updateMigrationRecord(migrationRecord);
          break;
        case CLEANING_BACKUP_FOR_COMPLETED:
        case CLEANING_INTERRUPTED_BACKUP:
          // This means the app was interrupted while backup data was being cleaned up. The state
          // machine will be unchanged since we just want to try cleaning up again.
          break;
        case MIGRATING:
          // This means the app was interrupted while data was being migrated. This will set the
          // state of the machine to “restoring” so that migration can be attempted again.
          migrationRecord.setLatestMigrationStatus(SyncMetadataMigrationStatus.RESTORING);
          updateMigrationRecord(migrationRecord);
          break;
        case RESTORING:
          // This means the app was interrupted while backup data was being restored. The state
          // machine will be unchanged since we just want to try restoring again.
          break;
        default:
          throw new IllegalStateException(
              "unsure of sync migration status, may be fixed by upgrading to latest version of "
                  + "Stitch SDK, or clearing app data"
          );
      }

      // Run the migration state machine until it has converged to a fully migrated state.
      while (true) {
        if (!doStateMachineTick()) {
          break;
        }
      }
    } finally {
      // migrationManagerLock.unlock();
    }
  }

  /**
   * Performs a tick of the state machine used to run migrations.
   *
   * @return true if another tick is necessary, returns false if the state machine has converged to
   * a fully migrated state
   */
  private boolean doStateMachineTick() {
    final SyncMetadataMigrationRecord migrationRecord = getLatestMigrationRecord();

    final SyncMetadataMigration currentMigration;

    switch (migrationRecord.getLatestMigrationStatus()) {
      case COMPLETED:
        // This means the current metadata version reflects the current status of the metadata,
        // and all backup data used to perform the migration is cleaned up.

        // If current_metadata_version is equal to the current metadata version for the SDK, then
        // no further migrations need to be performed, so the machine will converge to this state.
        if (migrationRecord.getCurrentMetadataVersion() == CURRENT_SDK_METADATA_VERSION) {
          return false;
        } else if (migrationRecord.getCurrentMetadataVersion() < CURRENT_SDK_METADATA_VERSION) {
          // If current_metadata_version is less than the current metadata version of the SDK, then
          // the next state of the machine will be “backing up”. This is because more migrations
          // will need to be performed.
          migrationRecord.setLatestMigrationStatus(SyncMetadataMigrationStatus.BACKING_UP);
          updateMigrationRecord(migrationRecord);
          return true;
        } else {
          // If current_metadata_version is greater than the current metadata version of the SDK,
          // then the migration manager will throw an IllegalStateException telling the developer
          // to upgrade their SDK or clear app data.
          throw new IllegalStateException(
              "MongoDB Stitch Sync metadata is using an unsupported version. may be fixed by "
                  + "upgrading to latest version of Stitch SDK, or clearing app data");
        }
      case BACKING_UP:
        // This means the migration manager should prepare to perform a migration by backing up any
        // data that might be needed to recover the documents to a pre-migration state. This will
        // run the performBackup method of the migration targeting (current_metadata_version + 1).
        currentMigration = migrations.get(migrationRecord.getCurrentMetadataVersion() + 1);
        currentMigration.performBackup();

        // When the backup is completed, the next state will be “migrating”.
        migrationRecord.setLatestMigrationStatus(SyncMetadataMigrationStatus.MIGRATING);
        updateMigrationRecord(migrationRecord);
        return true;
      case CLEANING_BACKUP_FOR_COMPLETED:
        // This means the migration manager should clean up any backup data that was created for a
        // migration because the migration was completed. This will run the cleanupBackup method of
        // the migration targeting current_metadata_version.
        currentMigration = migrations.get(migrationRecord.getCurrentMetadataVersion());
        currentMigration.cleanupBackup();

        // When the cleanup is completed, the next state will be “completed”/
        migrationRecord.setLatestMigrationStatus(SyncMetadataMigrationStatus.COMPLETED);
        updateMigrationRecord(migrationRecord);
        return true;
      case CLEANING_INTERRUPTED_BACKUP:
        // This means the migration manager should clean the backup data for a migration because
        // the backup was interrupted. This will run the cleanBackup method of the migration
        // targeting (current_metadata_version + 1).
        currentMigration = migrations.get(migrationRecord.getCurrentMetadataVersion() + 1);
        currentMigration.cleanupBackup();

        // When the clean is completed, the next state will be “backing_up”.
        migrationRecord.setLatestMigrationStatus(SyncMetadataMigrationStatus.BACKING_UP);
        updateMigrationRecord(migrationRecord);
        return true;
      case MIGRATING:
        // This means the migration manager should perform a migration. This will run the
        // performMigration method of the migration targeting (current_metadata_version + 1).
        currentMigration = migrations.get(migrationRecord.getCurrentMetadataVersion() + 1);
        currentMigration.performMigration();

        // When the migration is completed, the next state will be “cleaning_for_completed”, with
        // the current_metadata_version incremented.
        migrationRecord.setCurrentMetadataVersion(migrationRecord.getCurrentMetadataVersion() + 1);
        migrationRecord.setLatestMigrationStatus(
            SyncMetadataMigrationStatus.CLEANING_BACKUP_FOR_COMPLETED
        );
        updateMigrationRecord(migrationRecord);
        return true;
      case RESTORING:
        // This means the migration manager should restore the backup data for a migration because
        // a migration was interrupted. This is so that any non-idempotent updates in the migration
        // can be safely executed again. This will run the restoreBackup method of the migration
        // targeting (current_metadata_version + 1).
        currentMigration = migrations.get(migrationRecord.getCurrentMetadataVersion() + 1);
        currentMigration.restoreFromBackup();

        // When the restore is completed, the next state will be “migrating”.
        migrationRecord.setLatestMigrationStatus(SyncMetadataMigrationStatus.MIGRATING);
        updateMigrationRecord(migrationRecord);
        return true;
      default:
        throw new IllegalStateException(
            "unsure of sync migration status, may be fixed by upgrading to latest version of "
                + "Stitch SDK, or clearing app data"
        );
    }
  }
}
