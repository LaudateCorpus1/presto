/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spark.execution;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.execution.DataDefinitionTask;
import com.facebook.presto.execution.QueryStateMachine;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkQueryExecution;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.transaction.TransactionInfo;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class PrestoSparkDataDefinitionExecution<T extends Statement>
        implements IPrestoSparkQueryExecution
{
    private static final Logger log = Logger.get(PrestoSparkDataDefinitionExecution.class);

    private final DataDefinitionTask<T> task;
    private final QueryStateMachine queryStateMachine;
    private final T statement;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final Metadata metadata;

    public PrestoSparkDataDefinitionExecution(
            DataDefinitionTask<T> task,
            QueryStateMachine queryStateMachine,
            T statement,
            TransactionManager transactionManager,
            AccessControl accessControl,
            Metadata metadata)
    {
        this.task = requireNonNull(task, "task is null");
        this.queryStateMachine = requireNonNull(queryStateMachine, "session is null");
        this.statement = requireNonNull(statement, "statement is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public List<List<Object>> execute()
    {
        log.info("Executing DDL statemt %s", statement.toString());

        ListenableFuture<?> future = task.execute(statement, transactionManager, metadata, accessControl, queryStateMachine, Collections.emptyList());

        Futures.addCallback(future, new FutureCallback<Object>()
        {
            @Override
            public void onSuccess(@Nullable Object result)
            {
                getFutureValue(transactionManager.asyncCommit(getTransactionInfo(queryStateMachine.getSession(), transactionManager).getTransactionId()));
            }

            @Override
            public void onFailure(Throwable e)
            {
                fail(e);
                throwIfInstanceOf(e, Error.class);
            }
        }, directExecutor());

        return Collections.emptyList();
    }

    private void fail(Throwable cause)
    {
        queryStateMachine.transitionToFailed(cause);
        queryStateMachine.updateQueryInfo(Optional.empty());
    }

    private static TransactionInfo getTransactionInfo(Session session, TransactionManager transactionManager)
    {
        Optional<TransactionInfo> transaction = session.getTransactionId()
                .flatMap(transactionManager::getOptionalTransactionInfo);
        checkState(transaction.isPresent(), "transaction is not present");
        checkState(transaction.get().isAutoCommitContext(), "transaction doesn't have auto commit context enabled");
        return transaction.get();
    }
}
