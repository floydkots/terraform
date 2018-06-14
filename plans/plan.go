package plans

import (
	"github.com/zclconf/go-cty/cty"
)

type Plan struct {
	VariableValues  map[string]cty.Value
	Changes         *Changes
	ProviderSHA256s map[string][]byte
}
