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

package com.google.cloud.spanner;

import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.ConnectionOptions;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

// This program tests the autocommit feature using a pre-created Cloud-Devel database.
//
// Prerequisite:
// - The mentioned Spanner database includes a pre-created table foo(key: INT64, value: INT64).
//
// For a quick run:
// - One thing that prevents a run from finishing in seconds is that, if the table does not already
//   include all keys in [0..NUM_PRE_INSERTED-1], these keys will be inserted. But this is a
//   one-time thing.
// - Another things that may cause the program to take several minutes to run is that
//   testPerformance() uses 12 ways each updating NUM_TRANSACTIONS_IN_PERF_TEST records. For a quick
//   run, it is recommended to temporarily reduce NUM_TRANSACTIONS_IN_PERF_TEST.
//
public class Donghui {
  // Pointer to a pre-created staging database with a table foo(key: INT64, value: INT64).
  private static final String HOST = "staging-wrenchworks.sandbox.googleapis.com";
  private static final String PROJECT = "span-cloud-testing";
  private static final String INSTANCE = "test-instance";
  private static final String DATABASE = "donghuiz-db";

  // SQL related.
  private static final String INSERT = "INSERT INTO foo (key, value) VALUES (@key, @key)";
  private static final String UPDATE = "UPDATE foo SET value=value+1 WHERE key=@key";
  private static final String SELECT = "SELECT value FROM foo WHERE key=@key";
  private static final String SUM =
      "SELECT SUM(value) FROM foo WHERE key>=0 AND key<@numPreInserted";
  private static final String COUNT =
      "SELECT COUNT(value) FROM foo WHERE key>=0 AND key<@numPreInserted";
  private static final String ALL_KEYS = "SELECT key FROM foo WHERE key>=0 AND key<@numPreInserted";
  private static final String PARAM_KEY = "key";
  private static final String PARAM_NUM_PRE_INSERTED = "numPreInserted";
  private static final long NUM_PRE_INSERTED = 1000;

  private static final int NUM_TRANSACTIONS_IN_PERF_TEST = 1000;

  private static final Random random = new Random();

  // A random key in [0..NUM_PRE_INSERTED-1].
  private static long randomKey() {
    long key = random.nextLong() % NUM_PRE_INSERTED;
    return key >= 0 ? key : -key;
  }

  private static void check(boolean condition, String message) throws SpannerException {
    if (!condition) {
      throw new RuntimeException("[CHECK FAILURE:] "+ message);
    }
  }

  private static Connection newConnection() {
    final ConnectionOptions options =
        ConnectionOptions.newBuilder()
            .setUri(String.format("cloudspanner://%s/projects/%s/instances/%s/databases/%s",
                HOST, PROJECT, INSTANCE, DATABASE))
            .build();
    return options.getConnection();
  }

  private static DatabaseClient newDatabaseClient() {
    final SpannerOptions options =
        SpannerOptions.newBuilder().setProjectId(PROJECT).setHost("https://" + HOST).build();
    final Spanner spanner = options.getService();
    final DatabaseId id = DatabaseId.of(PROJECT, INSTANCE, DATABASE);
    return spanner.getDatabaseClient(id);
  }

  // Ensures all keys in [0..NUM_PRE_INSERTED-1] exist.
  private static void preInsert(DatabaseClient client) {
    HashSet<Long> keys = new HashSet<>();
    ResultSet resultSet = client.readOnlyTransaction().executeQuery(
        Statement.newBuilder(ALL_KEYS).bind(PARAM_NUM_PRE_INSERTED).to(NUM_PRE_INSERTED).build());
    while (resultSet.next()) {
      long key = resultSet.getLong(0);
      keys.add(key);
    }

    // Inserts all missing keys.
    client
        .readWriteTransaction()
        .run(
            transaction -> {
              for (long i = 0; i < NUM_PRE_INSERTED; ++i) {
                if (!keys.contains(i)) {
                  transaction.executeUpdate(Statement.newBuilder(INSERT).bind(PARAM_KEY).to(i).build());
                }
              }
              return null;
            });

    // Verifies all keys should exist.
    resultSet =
        client
            .readOnlyTransaction()
            .executeQuery(
                Statement.newBuilder(COUNT)
                    .bind(PARAM_NUM_PRE_INSERTED)
                    .to(NUM_PRE_INSERTED)
                    .build());
    check(
        resultSet.next() && resultSet.getLong(0) == NUM_PRE_INSERTED,
        "The number of pre-inserted keys is wrong.");
  }

  // Returns the value of a record given an existing key, or throws NOT_FOUND.
  public static long select(Connection connection, long key) throws SpannerException {
    check(!connection.isTransactionStarted(), "before selectOne, connection isTransactionStarted");
    ResultSet resultSet =
        connection.executeQuery(Statement.newBuilder(SELECT).bind(PARAM_KEY).to(key).build());
    check(resultSet.next(), "The record with key " + Long.toString(key) + " does not exist.");
    final long value = resultSet.getLong(0);
    resultSet.close();
    if (!connection.isAutocommit()) {
      connection.commit();
    }
    check(!connection.isTransactionStarted(), "after selectOne, connection isTransactionStarted");
    return value;
  }

  // Returns the SUM of values.
  public static long sum(Connection connection) throws SpannerException {
    return sumCountImpl(connection, true);
  }

  // Returns the COUNT of values.
  public static long count(Connection connection) throws SpannerException {
    return sumCountImpl(connection, false);
  }

  public static long sumCountImpl(Connection connection, boolean isSum) throws SpannerException {
    check(!connection.isTransactionStarted(), "before sum, connection isTransactionStarted");

    ResultSet resultSet = connection.executeQuery(
        Statement.newBuilder(isSum ? SUM : COUNT).bind(PARAM_NUM_PRE_INSERTED).to(NUM_PRE_INSERTED).build());
    check(resultSet.next(), "Fail to get result.");
    final long ret = resultSet.getLong(0);
    resultSet.close();
    if (!connection.isAutocommit()) {
      connection.commit();
    }
    check(!connection.isTransactionStarted(), "after selectOne, connection isTransactionStarted");
    return ret;
  }

  // Increments the value of a record with a given key.
  private static void updateOne(Connection connection, long key) {
    check(!connection.isTransactionStarted(), "isTransactionStarted should not be true");
    long row_count =
        connection.executeUpdate(Statement.newBuilder(UPDATE).bind(PARAM_KEY).to(key).build());
    check(row_count == 1, "Wrong updated row_count.");
    if (!connection.isAutocommit()) {
      connection.commit();
    }
    check(!connection.isTransactionStarted(), "isTransactionStarted should not be true");
  }

  // Increments the value of numUpdates pre-inserted records with randomly-selected keys.
  // Every update uses a separate transaction. Ensures exactly 1 row is modified.
  public static void update(Connection connection, boolean autocommit, int numUpdates)
      throws SpannerException {
    connection.setAutocommit(autocommit);
    for (int i = 0; i < numUpdates; ++i) {
      updateOne(connection, randomKey());
    }
  }

  private static void updateOne(DatabaseClient client, boolean inlineCommit, long key) {
    TransactionRunner runner =
        inlineCommit
            ? client.readWriteTransaction(Options.inlineCommitOption())
            : client.readWriteTransaction();
    runner.run(
        transaction -> {
          long row_count =
              transaction.executeUpdate(Statement.newBuilder(UPDATE).bind(PARAM_KEY).to(key).build());
          check(row_count == 1, "Wrong updated row_count.");
          return null;
        });
  }

  public static void update(DatabaseClient client, boolean inlineCommit, int numUpdates)
      throws SpannerException {
    for (int i = 0; i < numUpdates; ++i) {
      updateOne(client, inlineCommit, randomKey());
    }
  }

  public static void testUpdateOneShouldPersist(Connection connection, DatabaseClient client)
      throws SpannerException {
    final long key = randomKey();
    final long value = select(connection, key);
    int numIncrements = 0;

    // With or without autocommit, updateOne() should increment the value.
    while (numIncrements < 4) {
      ++numIncrements;
      connection.setAutocommit(numIncrements % 2 == 0);
      updateOne(connection, key);
      check(select(connection, key) == value + numIncrements, "wrong value");
    }

    // Same with DatabaseClient about updateOne().
    while (numIncrements < 8) {
      ++numIncrements;
      updateOne(client, numIncrements % 2 == 0, key);
      check(select(connection, key) == value + numIncrements, "wrong value");
    }

    System.out.println("testUpdateOneShouldPersist passed.");
  }

  public static void testUpdateShouldPersist(Connection connection, DatabaseClient client)
      throws SpannerException {
    final int numUpdates = 5;
    final long sum = sum(connection);
    long numIncrements = 0;
    long newSum = 0;

    numIncrements += numUpdates;
    update(connection, false, numUpdates);
    newSum = sum(connection);
    check(newSum == sum + numIncrements, "wrong value");

    numIncrements += numUpdates;
    update(connection, true, numUpdates);
    newSum = sum(connection);
    check(newSum == sum + numIncrements, "wrong value");

    numIncrements += numUpdates;
    update(client, false, numUpdates);
    newSum = sum(connection);
    check(newSum == sum + numIncrements, "wrong value");

    numIncrements += numUpdates;
    update(client, true, numUpdates);
    newSum = sum(connection);
    check(newSum == sum + numIncrements, "wrong value");

    System.out.println("testUpdateShouldPersist passed.");
  }

  private static void testCannotUpdateOrQueryAfterInlineCommit(
      Connection connection, DatabaseClient client) {
    // Thiago's test case of using multiple statements with inlineCommit
    // https://gist.github.com/thiagotnunes/b0a2a367a82c613116de8ba03a7c240c
    final long key = randomKey();
    final long value = select(connection, key);

    client
        .readWriteTransaction(Options.inlineCommitOption())
        .run(
            transaction -> {
              Statement dml = Statement.newBuilder(UPDATE).bind(PARAM_KEY).to(key).build();
              // The first update is ok and will lead the transaction to be committed.
              long row_count = transaction.executeUpdate(dml);
              check(row_count == 1, "Wrong updated row_count.");

              // An update after inlineCommit is expected to fail.
              try {
                transaction.executeUpdate(dml);
                check(false, "update after inlineCommit should fail");
              } catch (SpannerException e) {
                // System.out.println("[Expected exception:]" + e.toString());
              }

              // A query after inlineCommit is expected to fail.
              try {
                transaction.executeQuery(Statement.newBuilder(SELECT).bind(PARAM_KEY).to(key).build());
                check(false, "query after inlineCommit should fail");
              } catch (SpannerException e) {
                // System.out.println("[Expected exception:]" + e.toString());
              }
              return null;
            });

    // The first update should have been committed.
    check(select(connection, key) == value + 1, "value not changed");

    System.out.println("testCannotUpdateOrQueryAfterInlineCommit passed.");
  }

  // Percentage of speedup from slow to fast.
  private static double speedupPct(long slow, long fast) {
    if (fast == 0) {
      return 0;
    }
    return (((double) slow / fast) - 1) * 100;
  }

  public static void testPerformance(Connection connection, DatabaseClient client) {
    for (int maxTransactionsPerIteration : Arrays.asList(1, 10, 100)) {
      // Record the previous sum of all values.
      final long prev_sum = sum(connection);

      // Run 4 * NUM_TRANSACTIONS_IN_PERF_TEST transactions.
      long connNoAuto = 0, connAuto = 0, clientNoAuto = 0, clientAuto = 0;
      int numFinished = 0;
      while (numFinished < NUM_TRANSACTIONS_IN_PERF_TEST) {
        final int numUpdates =
            Math.min(maxTransactionsPerIteration, NUM_TRANSACTIONS_IN_PERF_TEST - numFinished);
        final long t1 = System.currentTimeMillis();
        update(connection, false, numUpdates);
        final long t2 = System.currentTimeMillis();
        update(connection, true, numUpdates);
        final long t3 = System.currentTimeMillis();
        update(client, false, numUpdates);
        final long t4 = System.currentTimeMillis();
        update(client, true, numUpdates);
        final long t5 = System.currentTimeMillis();

        connNoAuto += t2 - t1;
        connAuto += t3 - t2;
        clientNoAuto += t4 - t3;
        clientAuto += t5 - t4;
        numFinished += numUpdates;
      }
      System.out.println(String.format(
          "testPerformance (using the total time of %d transactions in batches of %d):\n"
              + "  Connection w/o autocommit: %.2fs; with autocommit: %.2fs; speedup: %.2f%%\n"
              + "  DatabaseClient w/o autocommit: %.2fs; with autocommit: %.2fs; speedup: %.2f%%\n",
          NUM_TRANSACTIONS_IN_PERF_TEST,
              maxTransactionsPerIteration,
              connNoAuto/1000.0, connAuto/1000.0, speedupPct(connNoAuto, connAuto),
              clientNoAuto/1000.0, clientAuto/1000.0, speedupPct(clientNoAuto, clientAuto)));

      // Ensure the overall increase of the SUM is expected.
      check(sum(connection) == prev_sum + NUM_TRANSACTIONS_IN_PERF_TEST * 4,
          "unexpected sum");
    }
  }

  public static void main(String[] args) throws SpannerException {
    Connection connection = newConnection();
    DatabaseClient client = newDatabaseClient();

    preInsert(client);
    testUpdateOneShouldPersist(connection, client);
    testUpdateShouldPersist(connection, client);
    testCannotUpdateOrQueryAfterInlineCommit(connection, client);
    testPerformance(connection, client);

    connection.close();
  }
}
