package _type

type CmdLine [][]byte
type Args [][]byte

// Entity stores data bound to a key, including a string, list, hash, set and so on
type Entity struct {
	Data any
}
