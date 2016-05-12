package server

func init() {
}

type (
	CommandHandler interface {
		SetHandle(cmd string, f func(...[]byte))
		RemoveHandle(cmd string)
	}
)
