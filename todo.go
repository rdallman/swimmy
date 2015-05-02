package swimmy

// TODO
// to provide persistent quorum in case somebody trips over the powercord at aws.
// we use 2 keys, make sure you don't conflict with them:
//	:::quorum:::
//	:::swimbers:::
//
// and store our last stable membership and quorum number.
//type StableStore interface {
//Put(k, v []byte) error
//Get(k []byte) ([]byte, error)
//}
