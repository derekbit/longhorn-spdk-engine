package spdk

import "github.com/cockroachdb/errors"

var (
	// ErrEngineFrontendCreateInvalidArgument indicates the create request carries
	// invalid input, such as an unparsable target address.
	ErrEngineFrontendCreateInvalidArgument = errors.New("engine frontend create invalid argument")
	// ErrEngineFrontendCreatePrecondition indicates the frontend is not in a
	// state that can satisfy create preconditions.
	ErrEngineFrontendCreatePrecondition = errors.New("engine frontend create precondition failed")
)
