package plsm

type Tx interface {
	Rollback()
}
