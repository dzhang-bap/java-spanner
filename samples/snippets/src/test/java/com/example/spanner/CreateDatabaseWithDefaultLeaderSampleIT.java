/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.spanner;

import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.InstanceConfig;
import org.junit.Ignore;
import org.junit.Test;

public class CreateDatabaseWithDefaultLeaderSampleIT extends SampleTestBase {

  @Ignore("Skipping until we have a MR instance to run this on")
  @Test
  public void testCreateDatabaseWithDefaultLeader() throws Exception {
    final String databaseId = idGenerator.generateDatabaseId();

    // Finds possible default leader
    final InstanceConfig config = instanceAdminClient.getInstanceConfig(instanceConfigName);
    assertTrue(
        "Expected instance config " + instanceConfigName + " to have at least one leader option",
        config.getLeaderOptions().size() > 0
    );
    final String defaultLeader = config.getLeaderOptions().get(0);

    // Runs sample
    final String out = SampleRunner.runSample(() -> CreateDatabaseWithDefaultLeaderSample
        .createDatabaseWithDefaultLeader(projectId, instanceId, databaseId, defaultLeader)
    );

    assertTrue(
        "Expected created database to have default leader " + defaultLeader + "."
            + " Output received was " + out,
        out.contains("Default leader: " + defaultLeader)
    );
  }
}
