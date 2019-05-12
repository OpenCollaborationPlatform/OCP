package replica

/* State to be replicated
 * - Will not be called from concurrent threads
 * - Execution haltet till return
 */
type State interface {

	//applying commands
	Apply([]byte)

	//snapshoting
	Snaphot() []byte
	LoadSnaphot([]byte) error
}
