package replica

import (
	"CollaborationNode/utils"
	"bytes"
	"encoding/binary"
	"encoding/json"
)

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

func newTestState() *testState {
	return &testState{make([]uint64, 0)}
}

//simple state that stores each uint given to apply
type testState struct {
	Value []uint64
}

//cmd must be a binary uint which is added to the current list of values
func (self *testState) Apply(cmd []byte) {

	buf := bytes.NewBuffer(cmd)
	val, err := binary.ReadUvarint(buf)
	if err != nil {
		self.Value = append(self.Value, val)
	}
}

func (self *testState) Snaphot() []byte {

	res, _ := json.Marshal(self.Value)
	return res
}

func (self *testState) LoadSnaphot(cmd []byte) error {

	self.Value = make([]uint64, 0)
	err := json.Unmarshal(cmd, &self.Value)
	if err != nil {
		return utils.StackError(err, "Unable to load snapshot")
	}

	return nil
}

func (self *testState) Equals(other *testState) bool {

	if len(self.Value) != len(other.Value) {
		return false
	}

	for i, val := range self.Value {
		if other.Value[i] != val {
			return false
		}
	}

	return true
}

func intToByte(val uint64) []byte {

	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf, val)
	return buf
}
