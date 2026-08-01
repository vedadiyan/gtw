package gtw

import (
	"testing"

	"github.com/vedadiyan/gtw/v2"
	"github.com/vedadiyan/gtw/v2/http"
	"github.com/vedadiyan/vedio"
)

type (
	TestAPI struct {
		gtw.Metadata `prefix:"api"`

		Test gtw.Service[int] `name:"test"`

		Get gtw.MessageHandler `route:"/test/:name" method:"GET"`
	}
)

func (t *TestAPI) GetHandler(req gtw.Message) (gtw.Message, error) {
	return gtw.Ok(gtw.JSON(map[string]any{"Hello": "World"}))
}

func TestParse(t *testing.T) {
	vedio.Register[int](vedio.WithName("test"), vedio.WithGenerator(func() (int, error) {
		return 0, nil
	}))
	gtw.Register(&TestAPI{})
	server := http.New(":8082")
	gtw.ListenAndServer(server)
}
