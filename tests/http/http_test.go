package gtw

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/vedadiyan/gtw/v2"
	"github.com/vedadiyan/gtw/v2/di"
	"github.com/vedadiyan/gtw/v2/websocket"
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
	di.AddSinletonWithName("test", func() (instance *int, err error) {
		i := 0
		return &i, nil
	})

	ttt := gtw.WebSocketMsg{
		Subject: "test",
		Data:    []byte("OK"),
	}

	xxx, _ := json.MarshalIndent(ttt, "", " ")

	fmt.Println(string(xxx))
	server := websocket.NewWebSocketServer(":8082")
	gtw.Register(&TestAPI{})
	gtw.ListenAndServer(server)
}
