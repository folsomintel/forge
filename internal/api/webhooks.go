package api

import (
	"context"
	"errors"
	"net/http"
	"net/url"

	"github.com/danielgtaylor/huma/v2"

	"github.com/folsomintel/forge/internal/auth"
	"github.com/folsomintel/forge/internal/repodb"
)

type WebhookWrite struct {
	URL    string   `json:"url,omitempty" format:"uri"`
	Secret string   `json:"secret,omitempty" doc:"HMAC signing secret; generated when omitted on create"`
	Events []string `json:"events,omitempty" doc:"Event names; * matches all. Default [push]"`
	Active *bool    `json:"active,omitempty"`
}

type WebhookWithSecret struct {
	repodb.Webhook
	Secret string `json:"secret" doc:"Shown once, on create"`
}

func validHookURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}

func (s *Server) registerWebhooks(api huma.API) {
	createOp := op("createWebhook", "POST", "/api/repos/{id}/webhooks", auth.ScopeRepoWrite,
		"Create a webhook")
	createOp.DefaultStatus = http.StatusCreated
	huma.Register(api, createOp, func(ctx context.Context, in *struct {
		ID   string `path:"id"`
		Body WebhookWrite
	}) (*struct{ Body WebhookWithSecret }, error) {
		if _, err := s.DB.GetRepo(ctx, in.ID); err != nil {
			return nil, huma.Error404NotFound("repository not found")
		}
		if !validHookURL(in.Body.URL) {
			return nil, huma.Error400BadRequest("url must be http(s)")
		}
		events := in.Body.Events
		if len(events) == 0 {
			events = []string{"push"}
		}
		hook := &repodb.Webhook{
			RepoID: in.ID, URL: in.Body.URL, Secret: in.Body.Secret,
			Events: events, Active: in.Body.Active == nil || *in.Body.Active,
		}
		if hook.Secret == "" {
			hook.Secret = repodb.NewID()
		}
		if err := s.DB.CreateWebhook(ctx, hook); err != nil {
			return nil, internalErr("create webhook", err)
		}
		return &struct{ Body WebhookWithSecret }{WebhookWithSecret{Webhook: *hook, Secret: hook.Secret}}, nil
	})

	huma.Register(api, op("listWebhooks", "GET", "/api/repos/{id}/webhooks", auth.ScopeRepoWrite,
		"List webhooks"),
		func(ctx context.Context, in *repoParam) (*struct{ Body []repodb.Webhook }, error) {
			hooks, err := s.DB.ListWebhooks(ctx, in.ID)
			if err != nil {
				return nil, internalErr("list webhooks", err)
			}
			if hooks == nil {
				hooks = []repodb.Webhook{}
			}
			return &struct{ Body []repodb.Webhook }{hooks}, nil
		})

	huma.Register(api, op("getWebhook", "GET", "/api/repos/{id}/webhooks/{hook}", auth.ScopeRepoWrite,
		"Get a webhook"),
		func(ctx context.Context, in *hookParam) (*struct{ Body repodb.Webhook }, error) {
			hook, err := s.DB.GetWebhook(ctx, in.ID, in.Hook)
			if errors.Is(err, repodb.ErrNotFound) {
				return nil, huma.Error404NotFound("webhook not found")
			}
			if err != nil {
				return nil, internalErr("get webhook", err)
			}
			return &struct{ Body repodb.Webhook }{*hook}, nil
		})

	huma.Register(api, op("updateWebhook", "PATCH", "/api/repos/{id}/webhooks/{hook}", auth.ScopeRepoWrite,
		"Update a webhook"),
		func(ctx context.Context, in *struct {
			ID   string `path:"id"`
			Hook string `path:"hook"`
			Body WebhookWrite
		}) (*struct{ Body repodb.Webhook }, error) {
			hook, err := s.DB.GetWebhook(ctx, in.ID, in.Hook)
			if errors.Is(err, repodb.ErrNotFound) {
				return nil, huma.Error404NotFound("webhook not found")
			}
			if err != nil {
				return nil, internalErr("get webhook", err)
			}
			if in.Body.URL != "" {
				if !validHookURL(in.Body.URL) {
					return nil, huma.Error400BadRequest("url must be http(s)")
				}
				hook.URL = in.Body.URL
			}
			if in.Body.Secret != "" {
				hook.Secret = in.Body.Secret
			}
			if len(in.Body.Events) > 0 {
				hook.Events = in.Body.Events
			}
			if in.Body.Active != nil {
				hook.Active = *in.Body.Active
			}
			if err := s.DB.UpdateWebhook(ctx, hook); err != nil {
				return nil, internalErr("update webhook", err)
			}
			return &struct{ Body repodb.Webhook }{*hook}, nil
		})

	delOp := op("deleteWebhook", "DELETE", "/api/repos/{id}/webhooks/{hook}", auth.ScopeRepoWrite,
		"Delete a webhook")
	delOp.DefaultStatus = http.StatusNoContent
	huma.Register(api, delOp, func(ctx context.Context, in *hookParam) (*struct{}, error) {
		err := s.DB.DeleteWebhook(ctx, in.ID, in.Hook)
		if errors.Is(err, repodb.ErrNotFound) {
			return nil, huma.Error404NotFound("webhook not found")
		}
		if err != nil {
			return nil, internalErr("delete webhook", err)
		}
		return nil, nil
	})

	huma.Register(api, op("listDeliveries", "GET", "/api/repos/{id}/webhooks/{hook}/deliveries", auth.ScopeRepoWrite,
		"List recent deliveries (30-day retention)"),
		func(ctx context.Context, in *hookParam) (*struct{ Body []repodb.Delivery }, error) {
			deliveries, err := s.DB.ListDeliveries(ctx, in.ID, in.Hook, 50)
			if err != nil {
				return nil, internalErr("list deliveries", err)
			}
			if deliveries == nil {
				deliveries = []repodb.Delivery{}
			}
			return &struct{ Body []repodb.Delivery }{deliveries}, nil
		})
}

type hookParam struct {
	ID   string `path:"id"`
	Hook string `path:"hook"`
}
