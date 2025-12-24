package gtw

import (
	"testing"

	"github.com/vedadiyan/gtw/v2"
	"github.com/vedadiyan/gtw/v2/di"
	"github.com/vedadiyan/gtw/v2/http"
)

type (
	TestAPI struct {
		gtw.Metadata `prefix:"api"`

		Test gtw.Service[int] `name:"test"`

		Get gtw.MessageHandler `route:"/test/:name" method:"GET"`
	}
)

func (t *TestAPI) GetHandler(req gtw.Message) (gtw.Message, error) {
	return gtw.Ok()
}

func TestParse(t *testing.T) {
	di.AddSinletonWithName("test", func() (instance *int, err error) {
		i := 0
		return &i, nil
	})
	server := http.New(":8082")
	gtw.Register(&TestAPI{})
	gtw.ListenAndServer(server)
}
