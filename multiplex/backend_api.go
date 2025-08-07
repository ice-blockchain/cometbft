package multiplex

import (
	"context"
	"fmt"
	"slices"
	"sync"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mempl "github.com/ice-blockchain/cometbft/mempool"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"github.com/ice-blockchain/cometbft/multiplex/types"
)

// ----------------------------------------------------------------------------
// types.Backend API implementation

// AddTransactions executes the CheckTx call to add individual
// transactions to the mempool by ChainID.
//
// This method is called by [BroadcastTx] when the transaction is ready
// to be broadcast to all other relays. Adding the transaction to the
// mempool effectively marks the transaction as locally accepted.
// AddTransactions implements [types.Backend].
func (b *MultiplexBackend) AddTransactions(
	userAddress string,
	transactions ...client.Transaction,
) error {
	broadcastID := client.GetBroadcastID(transactions...)
	for _, transaction := range transactions {
		chainID := client.GetChainID(userAddress, transaction.Fingerprint)
		clogger := b.logger.With(
			"requestId", broadcastID,
			"chainId", chainID,
		)

		memplReactor, ok := b.resourceMgr.Get(
			chainID,
			types.ServiceKeyMempoolReactor,
		).(*mempl.Reactor)
		if !ok {
			return fmt.Errorf(
				"failed to get local mempool; AddTransactions with ChainID %s", chainID)
		}
		chainMempool := memplReactor.GetMempoolPtr()

		// CheckTx locks the mempool update mutex.
		checkTxRes, err := chainMempool.CheckTx(
			client.TransactionToRawTx(transaction),
			b.nodeKey.ID(),
		)
		if err != nil {
			switch {
			case err == mempl.ErrTxInCache:
			case err == mempl.ErrTxInMempool:
			case err == mempl.ErrTxAlreadyReceivedFromSender:
				continue
			default:
				return err
			}
		}

		// Inform about local mempool addition result
		clogger.Info("Received CheckTx response", "res", checkTxRes)
	}

	return nil
}

// RemoveTransactions remove a transaction from the local mempool
// if it has been added already, e.g. using addTransactionToMempool.
//
// This method is called by [BroadcastTx] when a transaction rollback must
// be executed due to some of the healthy relays not accepting a batch.
//
// RemoveTransactions implements [types.Backend].
func (b *MultiplexBackend) RemoveTransactions(
	userAddress string,
	transactions ...client.Transaction,
) error {
	broadcastID := client.GetBroadcastID(transactions...)
	for _, transaction := range transactions {
		chainID := client.GetChainID(userAddress, transaction.Fingerprint)
		clogger := b.logger.With(
			"requestId", broadcastID,
			"chainId", chainID,
		)

		memplReactor, ok := b.resourceMgr.Get(
			chainID,
			types.ServiceKeyMempoolReactor,
		).(*mempl.Reactor)
		if !ok {
			return fmt.Errorf(
				"failed to get local mempool; RemoveTransactions with ChainID %s", chainID)
		}
		chainMempool := memplReactor.GetMempoolPtr()

		memTx := client.TransactionToRawTx(transaction)
		chainMempool.Lock()
		if err := chainMempool.RemoveTxByKey(memTx.Key()); err != nil {
			clogger.Debug("Rollback transaction not in local mempool (not an error)",
				"tx", cmtlog.NewLazySprintf("%X", memTx.Hash()),
				"error", err.Error())
		}
		chainMempool.Unlock()
	}

	return nil
}

// CancelBroadcastOperation executes the CancelBroadcast routine
// and the RemoveTransactions method to remove transactions
// from the local mempool.
func (b *MultiplexBackend) CancelBroadcastOperation(
	ctx context.Context,
	userAddress string,
	transactions ...client.Transaction,
) error {
	broadcastID := client.GetBroadcastID(transactions...)

	routineCancelBroadcast := b.Routines().CancelBroadcast
	go routineCancelBroadcast(ctx,
		userAddress,
		transactions,
		b.logger.With(
			"requestId", broadcastID,
			"txBatch", txHashesToHex(transactions...),
		),
	)

	return b.RemoveTransactions(userAddress, transactions...)
}

// OnBroadcastError updates a runtime completion status and attaches
// an error which happened during the consensus instance.
//
// All node runtimes activated by the consensus instance should be
// marked as completed with error.
func (b *MultiplexBackend) OnBroadcastError(
	reason error,
	userAddress string,
	transactions ...client.Transaction,
) error {
	// Track active runtimes and relax some resources with idle manager.
	relevantChainIds := chainIdsFromTransactions(userAddress, transactions...)
	transactionHashes := txHashesToHex(transactions...)
	broadcastID := client.GetBroadcastID(transactions...)

	// TODO(midas): remove debug logs
	b.logger.Debug("Node runtimes will be marked as completed with error",
		"requestId", broadcastID,
		"numNetworks", len(relevantChainIds),
		"chainIds", relevantChainIds,
		"txBatch", transactionHashes,
		"err", reason,
	)

	// Completes the runtimes activated by client.BroadcastTx.
	for _, chainID := range relevantChainIds {
		b.IdleManager().OnComplete(chainID)
	}

	return reason
}

// OnBroadcastComplete updates a runtime completion status. It accepts a batch
// of transactions and a list of remoteRelays that we may have to wait for
// until they have completed replication.
//
// When replication is announced as completed for all the relays currently
// catching up AND when all transactions are indexed, we proceed to mark the
// active runtime as completed, i.e. `OnComplete` is executed.
//
// Node runtimes that are not currently processing replications may be safely
// completed upon querying the indexerService until all transactions
// are included, and then proceed to mark the active runtime as completed.
func (b *MultiplexBackend) OnBroadcastComplete(
	ctx context.Context,
	userAddress string,
	remoteRelays []*helpers.RelayAddress,
	transactions ...client.Transaction,
) error {
	// Track active runtimes and relax some resources with idle manager.
	relevantChainIds := chainIdsFromTransactions(userAddress, transactions...)
	transactionHashes := txHashesToHex(transactions...)
	transactionsByChain := mapTransactionsByChainID(userAddress, transactions...)
	broadcastID := client.GetBroadcastID(transactions...)

	syncingPeersChainIds := []string{}
	totalNumReplications := 0
	for _, chainID := range relevantChainIds {
		// Did we send any ChainReplicationRequest for this ChainiD?
		chainReplRequests := b.replicationMgr.Requests(chainID)
		if len(chainReplRequests) == 0 {
			continue
		}

		totalNumReplications += len(chainReplRequests)
		syncingPeersChainIds = append(syncingPeersChainIds, chainID)
	}

	// Removes syncing chains, as we won't need to check their transactions.
	relevantChainIds = slices.DeleteFunc(relevantChainIds, func(cid string) bool {
		return slices.Contains(syncingPeersChainIds, cid)
	})

	b.logger.Info("Now evaluating consensus instance completion",
		"requestId", broadcastID,
		"numNetworks", len(relevantChainIds),
		"numRemotes", len(remoteRelays),
		"numSyncingChainIds", len(syncingPeersChainIds),
		"totalNumReplications", totalNumReplications,
		"txBatch", transactionHashes,
	)

	// THREAD #1:
	//
	// If any replication (sync) is in progress for one of the relevant
	// ChainID values, then we must wait for completion before we may idle.
	if len(syncingPeersChainIds) > 0 {
		b.logger.Info("Delaying the idle manager until relays have caught up",
			"requestId", broadcastID,
			"numNetworks", len(syncingPeersChainIds),
			"chainIds", syncingPeersChainIds,
			"txBatch", transactionHashes,
		)

		// Wait for the syncing relays to announce a ChainReplicationComplete.
		// Additionally, we make sure txes are all indexed after completion.
		go b.waitForChainReplications(ctx,
			syncingPeersChainIds,
			transactionsByChain,
		)
	}

	// THREAD #2:
	//
	// All other relays participated in consensus, thus transactions for
	// these ChainID should have been indexed by now, if they were not
	// we shall be listening for transaction events, i.e. `EventQueryTx`.

	go b.waitForIndexedTransactions(ctx,
		relevantChainIds,
		transactionsByChain,
	)

	msgSuccess := "Consensus instance completed"
	if len(syncingPeersChainIds) > 0 {
		msgSuccess += " - some relays are replicating in background"
	}

	// TODO(midas): remove debug logs
	b.logger.Debug(msgSuccess,
		"requestId", broadcastID,
		"numNetworks", len(relevantChainIds),
		"numRemotes", len(remoteRelays),
		"numWaiting", len(syncingPeersChainIds),
		"numSyncing", totalNumReplications,
		"txBatch", transactionHashes,
	)

	return nil
}

// ----------------------------------------------------------------------------

// waitForIndexedTransactions creates goroutines that wait for indexing events
// with relevantChainIds and all transactions for each ChainID.
//
// The main thread is blocked using a WaitGroup, and this method completes
// only when *all* transactions for relevantChainIds are indexed.
//
// Additionally, runtimes are marked complete when all txes are indexed.
func (b *MultiplexBackend) waitForIndexedTransactions(
	ctx context.Context,
	relevantChainIds []string,
	transactionsByChain map[string][]client.Transaction,
) (numCompleted int) {
	chainsWg := new(sync.WaitGroup)
	chainsWg.Add(len(relevantChainIds))

	for _, chainID := range relevantChainIds {
		cliTxes := transactionsByChain[chainID]

		// One goroutine per syncing ChainID, blocked until all txes indexed.
		// The runtime is marked complete upon completion of all tx indexing.
		go func() {
			defer chainsWg.Done()
			defer func() {
				b.IdleManager().OnComplete(chainID)
			}()

			txesWg := new(sync.WaitGroup)
			txesWg.Add(len(cliTxes))

			for _, tx := range cliTxes {
				// One goroutine per txHash, blocked until tx indexed.
				go func() {
					defer txesWg.Done()

					txHash := txHashesToHex(tx)[0]
					if ok := b.broadcastMgr.WaitIndexed(txHash); ok {
						numCompleted++
					}
				}()
			}
			txesWg.Wait()
		}()
	}
	chainsWg.Wait()

	return // numCompleted
}

// waitForChainReplications creates goroutines that wait for replications
// with relevantChainIds and all transactions for each ChainID.
//
// The main thread is blocked using a WaitGroup, and this method completes
// only when *all* transactions for relevantChainIds are also indexed.
//
// Eventually, it should execute waitForIndexedTransactions upon deferral.
func (b *MultiplexBackend) waitForChainReplications(
	ctx context.Context,
	relevantChainIds []string,
	transactionsByChain map[string][]client.Transaction,
) (numCompleted int) {
	defer b.waitForIndexedTransactions(ctx,
		relevantChainIds,
		transactionsByChain,
	)

	// This goroutine will be locked until relevant relays are done with replication.
	completionWg := new(sync.WaitGroup)
	completionWg.Add(len(relevantChainIds))
	for _, syncingChainID := range relevantChainIds {
		go func() {
			defer completionWg.Done()

			// This blocks the goroutine until shutdown and/or replication done.
			if ok := b.replicationMgr.WaitCompleted(syncingChainID); ok {
				numCompleted++
			}
		}()
	}
	completionWg.Wait()

	// TODO(midas): remove debug logs
	b.logger.Debug("All relays have caught up and completed chain replications",
		"numNetworks", len(relevantChainIds),
		"numSynced", numCompleted,
		"chainIds", relevantChainIds,
	)

	return // numCompleted
}
