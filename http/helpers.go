package http

import (
	"fmt"
	"strings"
)

func toGoRouteTemplate(route string) string {
	segments := strings.Split(route, "/")
	for i, v := range segments {
		if strings.HasPrefix(v, ":") {
			segments[i] = fmt.Sprintf("{%s}", strings.TrimPrefix(v, ":"))
		}
	}
	return strings.Join(segments, "/")
}
