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

import java.util.HashMap;
import java.util.Map;

public enum SyncMetadataMigrationStatus {
  COMPLETED("completed"),
  BACKING_UP("backing_up"),
  CLEANING_BACKUP_FOR_COMPLETED("cleaning_backup_for_completed"),
  CLEANING_INTERRUPTED_BACKUP("cleaning_interrupted_backup"),
  MIGRATING("migrating"),
  RESTORING("restoring"),
  UNKNOWN("unknown");

  SyncMetadataMigrationStatus(final String code) {
    this.code = code;
  }

  private final String code;

  public String getCode() {
    return code;
  }

  private static final Map<String, SyncMetadataMigrationStatus> CODE_NAME_TO_STATUS =
      new HashMap<>();

  static {
    CODE_NAME_TO_STATUS.put(COMPLETED.code, COMPLETED);
    CODE_NAME_TO_STATUS.put(BACKING_UP.code, BACKING_UP);
    CODE_NAME_TO_STATUS.put(CLEANING_BACKUP_FOR_COMPLETED.code, CLEANING_BACKUP_FOR_COMPLETED);
    CODE_NAME_TO_STATUS.put(CLEANING_INTERRUPTED_BACKUP.code, CLEANING_INTERRUPTED_BACKUP);
    CODE_NAME_TO_STATUS.put(MIGRATING.code, MIGRATING);
    CODE_NAME_TO_STATUS.put(RESTORING.code, RESTORING);
  }

  public static SyncMetadataMigrationStatus fromStatusCode(final String code) {
    if (!CODE_NAME_TO_STATUS.containsKey(code)) {
      return UNKNOWN;
    }
    return CODE_NAME_TO_STATUS.get(code);
  }
}
