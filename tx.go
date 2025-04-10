package mini_lsm

type Tx interface {
	Rollback()
}
