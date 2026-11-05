//go:build !darwin

package localgateway

import (
	"context"
	"errors"
)

type AliasManager struct{}

func NewAliasManager() *AliasManager { return &AliasManager{} }

// RecoverAliases is an explicit recovery operation available only on Darwin.
func RecoverAliases(context.Context, []string) error {
	return errors.New("local IPv4 alias recovery is supported only on Darwin")
}

func (*AliasManager) Acquire(context.Context, string) error {
	return errors.New("local IPv4 aliases are supported only on Darwin")
}

func (*AliasManager) Release(context.Context, string) error {
	return errors.New("local IPv4 aliases are supported only on Darwin")
}

func (*AliasManager) OwnedIPs() []string { return nil }

func (*AliasManager) Close(context.Context) error { return nil }
