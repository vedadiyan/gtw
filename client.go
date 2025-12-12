package gtw

type (
	Client interface {
		Call(Pattern, *Message) (*Message, error)
	}
)
