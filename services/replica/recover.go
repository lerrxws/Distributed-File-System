package replica

import (
	"context"
	"fmt"

	replicaApi "dfs/proto-gen/replica"
	utils "dfs/utils"
	fl "dfs/utils/filelogger"

	"github.com/cihub/seelog"
)

type RecoveryModule struct {
	logger     seelog.LoggerInterface
	filelogger *fl.JsonFileLogger
	
	// Callbacks to interact with replica state
	getExecutedMethods func() []*replicaApi.MethodRequest
	addExecutedMethod  func(*replicaApi.MethodRequest)
	executeMethod      func(context.Context, *replicaApi.MethodRequest) (*replicaApi.ExecuteMethodResponse, error)
	logExecutedMethod  func(*replicaApi.MethodRequest) error
}

func NewRecoveryModule(
	logger seelog.LoggerInterface,
	filelogger *fl.JsonFileLogger,
	getExecutedMethods func() []*replicaApi.MethodRequest,
	addExecutedMethod func(*replicaApi.MethodRequest),
	executeMethod func(context.Context, *replicaApi.MethodRequest) (*replicaApi.ExecuteMethodResponse, error),
	logExecutedMethod func(*replicaApi.MethodRequest) error,
) *RecoveryModule {
	return &RecoveryModule{
		logger:             logger,
		filelogger:         filelogger,
		getExecutedMethods: getExecutedMethods,
		addExecutedMethod:  addExecutedMethod,
		executeMethod:      executeMethod,
		logExecutedMethod:  logExecutedMethod,
	}
}

func (rm *RecoveryModule) RecoverFromPrimary(primaryAddr string, isPrimary bool) error {
	if isPrimary {
		rm.logger.Infof("[Recovery] This replica is already the primary, skipping recovery.")
		return nil
	}

	primaryClient, err := utils.ConnectToReplicaClient(primaryAddr)
	if err != nil {
		return rm.logger.Errorf("[Recovery] Failed to connect to primary node %s: %v", primaryAddr, err)
	}

	primaryMethods, err := rm.fetchExecutedMethodsFromPrimary(primaryClient)
	if err != nil {
		return rm.logger.Errorf("recovery failed: %v", err)
	}

	if len(primaryMethods) == 0 {
		rm.logger.Infof("[Recovery] No methods to recover from primary node.")
		return nil
	}

	if err := rm.validateHistoryConsistency(primaryMethods); err != nil {
		return fmt.Errorf("history validation failed: %v", err)
	}

	if err := rm.replayMissingMethods(primaryMethods); err != nil {
		return fmt.Errorf("replay failed: %v", err)
	}

	rm.logger.Infof("[Recovery] Recovery completed successfully.")
	return nil
}

func (rm *RecoveryModule) handleRecoverFromPrimary(primaryAddr string, isPrimary bool) error {
	if isPrimary {
		rm.logger.Infof("[Recovery] This replica is already the primary, skipping recovery.")
		return nil
	}

	primaryClient, err := utils.ConnectToReplicaClient(primaryAddr)
	if err != nil {
		return rm.logger.Errorf("[Recovery] Failed to connect to primary node %s: %v", primaryAddr, err)
	}

	primaryMethods, err := rm.fetchExecutedMethodsFromPrimary(primaryClient)
	if err != nil {
		return rm.logger.Errorf("recovery failed: %v", err)
	}

	if len(primaryMethods) == 0 {
		rm.logger.Infof("[Recovery] No methods to recover from primary node.")
		return nil
	}

	if err := rm.validateHistoryConsistency(primaryMethods); err != nil {
		return fmt.Errorf("history validation failed: %v", err)
	}

	if err := rm.replayMissingMethods(primaryMethods); err != nil {
		return fmt.Errorf("replay failed: %v", err)
	}

	rm.logger.Infof("[Recovery] Recovery completed successfully.")
	return nil
}

// region FETCH PHASE - Retrieving Primary State
func (rm *RecoveryModule) fetchExecutedMethodsFromPrimary(
	primaryClient replicaApi.ReplicaServiceClient,
) ([]*replicaApi.MethodRequest, error) {
	resp, err := primaryClient.GetExecutedMethods(context.Background(), &replicaApi.GetExecutedMethodsRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve executed methods from primary: %v", err)
	}

	if resp == nil {
		return []*replicaApi.MethodRequest{}, nil
	}

	return resp.MethodRequests, nil
}

// endregion

// region VALIDATION PHASE - Ensuring Consistency

func (rm *RecoveryModule) validateHistoryConsistency(primaryMethods []*replicaApi.MethodRequest) error {
	localMethods := rm.getExecutedMethods()
	localCount := len(localMethods)
	primaryCount := len(primaryMethods)

	if localCount > primaryCount {
		return fmt.Errorf("replica has more methods (%d) than primary (%d)", localCount, primaryCount)
	}

	// Validate that existing local history matches primary
	for i, localMethod := range localMethods {
		primaryMethod := primaryMethods[i]

		if err := rm.validateMethodMatch(localMethod, primaryMethod, i); err != nil {
			return err
		}
	}

	return nil
}

func (rm *RecoveryModule) validateMethodMatch(
	local, primary *replicaApi.MethodRequest,
	index int,
) error {
	// Check method name
	if local.MethodName != primary.MethodName {
		return fmt.Errorf(
			"method name mismatch at index %d: local='%s' vs primary='%s'",
			index, local.MethodName, primary.MethodName,
		)
	}

	// Check parameter count
	if len(local.MethodParameters) != len(primary.MethodParameters) {
		return fmt.Errorf(
			"parameter count mismatch at index %d for method '%s': local=%d vs primary=%d",
			index, local.MethodName, len(local.MethodParameters), len(primary.MethodParameters),
		)
	}

	// Check each parameter
	for paramIdx, localParam := range local.MethodParameters {
		primaryParam := primary.MethodParameters[paramIdx]
		if localParam != primaryParam {
			return fmt.Errorf(
				"parameter mismatch at index %d, param %d for method '%s': local='%s' vs primary='%s'",
				index, paramIdx, local.MethodName, localParam, primaryParam,
			)
		}
	}

	return nil
}

// endregion

// region REPLAY PHASE - Executing Missing Operations

func (rm *RecoveryModule) replayMissingMethods(primaryMethods []*replicaApi.MethodRequest) error {
	localMethods := rm.getExecutedMethods()
	startIndex := len(localMethods)
	missingMethods := primaryMethods[startIndex:]

	rm.logger.Infof(
		"[Recovery] Replaying %d missing methods (starting from index %d)",
		len(missingMethods), startIndex,
	)

	for i, method := range missingMethods {
		if _, err := rm.executeMethod(context.Background(), method); err != nil {
			return fmt.Errorf("failed to execute method at index %d: %v", startIndex+i, err)
		}

		rm.addExecutedMethod(method)

		if err := rm.logExecutedMethod(method); err != nil {
			rm.logger.Errorf("[Recovery] Failed to log method '%s': %v", method.MethodName, err)
		}

		rm.logger.Infof("[Recovery] Synchronized method %d/%d: %s", i+1, len(missingMethods), method.MethodName)
	}

	return nil
}

// endregion