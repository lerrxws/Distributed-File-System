package replica

import (
	"context"

	replicaApi "dfs/proto-gen/replica"
)

func (r *ReplicaServiceServer) recover() {
	r.isRecovered = false

	primaryAddr := r.manager.GetPrimaryNodeAddr()

	primaryClient, err := r.connectToPrimary(primaryAddr)
	if err != nil {
		r.logger.Errorf("[Replica] Failed to connect to primary node %s: %v", primaryAddr, err)
	}

	err = r.recoverFromPrimary(primaryClient)

	if err == nil {
		r.isRecovered = true
	}
}

func (r *ReplicaServiceServer) recoverFromPrimary(primaryClient replicaApi.ReplicaServiceClient) error{
	if r.manager.IsPrimary {
		r.logger.Infof("[Replica] This replica is already the primary, skipping recovery.")

		return nil
	}

	resp, err := primaryClient.GetExecutedMethods(context.Background(), &replicaApi.GetExecutedMethodsRequest{})
	if err != nil {
		return r.logger.Errorf("[Replica] Failed to retrieve executed methods from primary node: %v", err)
	}

	if resp == nil || len(resp.MethodRequests) == 0 {
        r.logger.Infof("[Replica] No methods to recover from primary node.")
        return nil
    }


	if conflict := r.checkConflictInExecutedMethods(resp.MethodRequests); conflict {
		return r.logger.Errorf("[Replica] Conflict detected during recovery.")
	}

	for _, method := range resp.MethodRequests {
		_, err := r.handleExecuteMethod(context.Background(), method)
		if err != nil {
			return r.logger.Errorf("[Replica] Failed to execute method %s: %v", method.MethodName, err)
		}

		r.logger.Infof("[Replica] Successfully executed method: %s", method.MethodName)
	}

	return nil
}

func (r *ReplicaServiceServer) checkConflictInExecutedMethods(primaryExecutedMethods []*replicaApi.MethodRequest) bool {
	if conflict := r.checkMethodsLength(primaryExecutedMethods); conflict {
		return true
	}

	for indM, method := range r.methodExecuted {
		primaryMethod := primaryExecutedMethods[indM]

		if conflict := r.checkMethodNameConflict(method, primaryMethod); conflict {
			return true
		}

		if conflict := r.checkMethodParametersLengthConflict(method, primaryMethod); conflict {
			return true
		}

		if conflict := r.checkMethodParametersConflict(method, primaryMethod); conflict {
			return true
		}
	}

	return false
}

func (r *ReplicaServiceServer) checkMethodsLength(primaryExecutedMethods []*replicaApi.MethodRequest) bool {
	if len(r.methodExecuted) > len(primaryExecutedMethods) {
		r.logger.Errorf("[Replica] The replica has more executed methods than the primary.")
		return true
	}
	return false
}

func (r *ReplicaServiceServer) checkMethodNameConflict(method, primaryMethod *replicaApi.MethodRequest) bool {
	if method.MethodName != primaryMethod.MethodName {
		r.logger.Errorf("[Replica] Method name mismatch: replica method '%s' vs primary method '%s'.", method.MethodName, primaryMethod.MethodName)
		return true
	}
	return false
}

func (r *ReplicaServiceServer) checkMethodParametersLengthConflict(method, primaryMethod *replicaApi.MethodRequest) bool {
	if len(method.MethodParameters) != len(primaryMethod.MethodParameters) {
		r.logger.Errorf("[Replica] Parameter count mismatch for method '%s': replica has %d parameters, primary has %d parameters.",
			method.MethodName, len(method.MethodParameters), len(primaryMethod.MethodParameters))
		return true
	}
	return false
}

func (r *ReplicaServiceServer) checkMethodParametersConflict(method, primaryMethod *replicaApi.MethodRequest) bool {
	for indP, parameter := range method.MethodParameters {
		primaryParameter := primaryMethod.MethodParameters[indP]
		if parameter != primaryParameter {
			r.logger.Errorf("[Replica] Parameter mismatch for method '%s': replica parameter '%v' vs primary parameter '%v'.",
				method.MethodName, parameter, primaryParameter)
			return true
		}
	}
	return false
}
