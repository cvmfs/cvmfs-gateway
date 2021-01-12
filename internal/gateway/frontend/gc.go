package frontend

import (
	"encoding/json"
	"net/http"

	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"
	"github.com/julienschmidt/httprouter"
)

// MakeGCHandler creates an HTTP handler for the "/gc" endpoint
func GCStartHandler(services be.ActionController) httprouter.Handle {
	return func(w http.ResponseWriter, h *http.Request, ps httprouter.Params) {
		token := ps.ByName("token")

		ctx := h.Context()

		var options be.GCOptions
		if err := json.NewDecoder(h.Body).Decode(&options); err != nil {
			httpWrapError(ctx, err, "invalid request body", w, http.StatusBadRequest)
			return
		}

		msg := map[string]interface{}{"status": "ok"}
		if err := services.StartGC(ctx, token, options); err != nil {
			msg["status"] = "error"
			msg["reason"] = err.Error()
		} else {
			msg["output"] = "GC started"
		}
		gw.LogC(ctx, "http", gw.LogInfo).Msg("request processed")

		replyJSON(ctx, w, msg)
	}
}

// MakeGCHandler creates an HTTP handler for the "/gc" endpoint
func GCCheckHandler(services be.ActionController) httprouter.Handle {
	return func(w http.ResponseWriter, h *http.Request, ps httprouter.Params) {
		token := ps.ByName("token")

		ctx := h.Context()

		msg := map[string]interface{}{"status": "in_progress"}
		if services.IsDoingGC(ctx, token) {
		} else {
			msg["status"] = "done"
		}

		gw.LogC(ctx, "http", gw.LogInfo).Msg("request processed")

		replyJSON(ctx, w, msg)
	}
}
