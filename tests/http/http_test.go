package gtw

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/vedadiyan/gtw/v2"
	"github.com/vedadiyan/gtw/v2/websocket"
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
