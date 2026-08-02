package gtw

import (
	"context"
	"testing"

	"github.com/vedadiyan/gtw/v2"
	"github.com/vedadiyan/gtw/v2/http"
	"github.com/vedadiyan/vedio"
)

type (
	Service struct {
		id int
	}

	ExpectedInterface interface {
		Run(context.Context, int)
	}

	TestAPI struct {
		gtw.Metadata `prefix:"api"`

		Test gtw.ServiceProxy[ExpectedInterface] `name:"test"`

		Get gtw.MessageHandler `route:"/test/:name" method:"GET"`
	}
)

func (s *Service) Init() error {
	s.id = 100
	return nil
}

func (s *Service) Run(ctx context.Context) error {
	return nil
}

func (t *TestAPI) GetHandler(req gtw.Message) (gtw.Message, error) {
	t.Test.Proxy(nil, context.TODO())
	return gtw.Ok(gtw.JSON(map[string]any{"Hello": "World"}))
}

func TestParse(t *testing.T) {
	vedio.RegisterFor[any, Service](vedio.WithName("test"))
	gtw.Register(&TestAPI{})
	server := http.New(":8082")
	gtw.ListenAndServer(server)
}
