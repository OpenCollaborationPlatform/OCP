package replica

import (
	"github.com/OpenCollaborationPlatform/OCP /utils"
)

/*Error handling*/

const Error_Authorisation = "authorisation_invalid" // error due to wrong authorisation
const Error_Unavailable = "node_unavailable"        // error due to not being able to reach node
const Error_Arguments = "arguments_wrong"           // arguments are invalid (e.g. wrong number, wrong type)
const Error_Operation_Invalid = "operation_invalid" // User operation not possible with current application state
const Error_Process = "process_failed"              // For all kind of proccesses or operations
const Error_Invalid_Data = "data_invalid"           // For all kind of wrong data errors
const Error_Setup = "setup_invalid"                 // The datastructures are not setup correctly

func newInternalError(reason, msg string, args ...interface{}) utils.OCPError {
	err := utils.NewError(utils.Internal, "replication", reason, args)
	if msg != "" {
		err.AddToStack(msg)
	}
	return err
}

func wrapInternalError(err error, reason string, args ...interface{}) error {
	if err != nil {
		if ocperr, ok := err.(utils.OCPError); ok {
			return ocperr
		} else {
			return newInternalError(reason, err.Error(), args...)
		}
	}
	return err
}

func newConnectionError(reason, msg string, args ...interface{}) utils.OCPError {
	err := utils.NewError(utils.Connection, "replication", reason, args)
	if msg != "" {
		err.AddToStack(msg)
	}
	return err
}

func wrapConnectionError(err error, reason string, args ...interface{}) error {
	if err != nil {
		if ocperr, ok := err.(utils.OCPError); ok {
			return ocperr
		} else {
			return newConnectionError(reason, err.Error(), args...)
		}
	}
	return err
}
