package googleapi

import (
	"context"
	"fmt"

	"google.golang.org/api/webmasters/v3"

	"github.com/steipete/gogcli/internal/googleauth"
)

func NewSearchConsole(ctx context.Context, email string) (*webmasters.Service, error) {
	if opts, err := optionsForAccount(ctx, googleauth.ServiceSearchConsole, email); err != nil {
		return nil, fmt.Errorf("search console options: %w", err)
	} else if svc, err := webmasters.NewService(ctx, opts...); err != nil {
		return nil, fmt.Errorf("create search console service: %w", err)
	} else {
		return svc, nil
	}
}
