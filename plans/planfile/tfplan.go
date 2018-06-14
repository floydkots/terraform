package planfile

import (
	"fmt"
	"io"
	"io/ioutil"

	"github.com/golang/protobuf/proto"
	"github.com/zclconf/go-cty/cty"

	"github.com/hashicorp/terraform/plans"
	"github.com/hashicorp/terraform/plans/internal/planproto"
	"github.com/hashicorp/terraform/version"
)

const tfplanFormatVersion = 3

// ---------------------------------------------------------------------------
// This file deals with the internal structure of the "tfplan" sub-file within
// the plan file format. It's all private API, wrapped by methods defined
// elsewhere. This is the only file that should import the
// ../internal/planproto package, which contains the ugly stubs generated
// by the protobuf compiler.
// ---------------------------------------------------------------------------

// readTfplan reads a protobuf-encoded description from the plan portion of
// a plan file, which is stored in a special file in the archive called
// "tfplan".
func readTfplan(r io.Reader) (*plans.Plan, error) {
	src, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	var rawPlan planproto.Plan
	err = proto.Unmarshal(src, &rawPlan)
	if err != nil {
		return nil, fmt.Errorf("parse error: %s", err)
	}

	if rawPlan.Version != tfplanFormatVersion {
		return nil, fmt.Errorf("unsupported plan file format version %d; only version %d is supported", rawPlan.Version, tfplanFormatVersion)
	}

	if rawPlan.TerraformVersion != version.String() {
		return nil, fmt.Errorf("plan file was created by Terraform %s, but this is %s; plan files cannot be transferred between different Terraform versions", rawPlan.TerraformVersion, version.String())
	}

	// TODO: Populate the rest of this!
	plan := &plans.Plan{
		VariableValues: map[string]cty.Value{},
		Changes: &plans.Changes{
			RootOutputs: map[string]*plans.OutputChange{},
			Resources:   []*plans.ResourceInstanceChange{},
		},

		ProviderSHA256s: map[string][]byte{},
	}

	for _, rawOC := range rawPlan.OutputChanges {
		name := rawOC.Name
		change, err := changeFromTfplan(rawOC.Change)
		if err != nil {
			return nil, fmt.Errorf("invalid plan for output %q: %s", name, err)
		}

		plan.Changes.RootOutputs[name] = &plans.OutputChange{
			Change:    *change,
			Sensitive: rawOC.Sensitive,
		}
	}

	return plan, nil
}

func changeFromTfplan(rawChange *planproto.Change) (*plans.Change, error) {
	if rawChange == nil {
		return nil, fmt.Errorf("change object is absent")
	}

	ret := &plans.Change{}

	// -1 indicates that there is no index. We'll customize these below
	// depending on the change action, and then decode.
	beforeIdx, afterIdx := -1, -1

	switch rawChange.Action {
	case planproto.Action_NOOP:
		ret.Action = plans.NoOp
		beforeIdx = 0
		afterIdx = 0
	case planproto.Action_CREATE:
		ret.Action = plans.Create
		afterIdx = 0
	case planproto.Action_READ:
		ret.Action = plans.Read
		beforeIdx = 0
		afterIdx = 1
	case planproto.Action_UPDATE:
		ret.Action = plans.Update
		beforeIdx = 0
		afterIdx = 1
	case planproto.Action_REPLACE:
		ret.Action = plans.Replace
		beforeIdx = 0
		afterIdx = 1
	case planproto.Action_DELETE:
		ret.Action = plans.Delete
		beforeIdx = 0
	default:
		return nil, fmt.Errorf("invalid change action %s", rawChange.Action)
	}

	if beforeIdx != -1 {
		if l := len(rawChange.Values); l <= beforeIdx {
			return nil, fmt.Errorf("incorrect number of values (%d) for %s change", l, rawChange.Action)
		}
		var err error
		ret.Before, err = valueFromTfplan(rawChange.Values[beforeIdx])
		if err != nil {
			return nil, fmt.Errorf("invalid \"before\" value: %s", err)
		}
		if ret.Before == nil {
			return nil, fmt.Errorf("missing \"before\" value: %s", err)
		}
	}
	if afterIdx != -1 {
		if l := len(rawChange.Values); l <= afterIdx {
			return nil, fmt.Errorf("incorrect number of values (%d) for %s change", l, rawChange.Action)
		}
		var err error
		ret.After, err = valueFromTfplan(rawChange.Values[afterIdx])
		if err != nil {
			return nil, fmt.Errorf("invalid \"after\" value: %s", err)
		}
		if ret.After == nil {
			return nil, fmt.Errorf("missing \"after\" value: %s", err)
		}
	}

	return ret, nil
}

func valueFromTfplan(rawV *planproto.DynamicValue) (plans.DynamicValue, error) {
	if rawV.Msgpack == nil {
		return nil, fmt.Errorf("dynamic value does not have msgpack serialization")
	}

	return plans.DynamicValue(rawV.Msgpack), nil
}

// writeTfplan serializes the given plan into the protobuf-based format used
// for the "tfplan" portion of a plan file.
func writeTfplan(plan *plans.Plan, w io.Writer) error {
	rawPlan := &planproto.Plan{
		Version:          tfplanFormatVersion,
		TerraformVersion: version.String(),
		ProviderHashes:   map[string]*planproto.Hash{},

		Variables:       map[string]*planproto.DynamicValue{},
		OutputChanges:   []*planproto.OutputChange{},
		ResourceChanges: []*planproto.ResourceInstanceChange{},
	}

	for name, oc := range plan.Changes.RootOutputs {
		// Writing outputs as cty.DynamicPseudoType forces the stored values
		// to also contain dynamic type information, so we can recover the
		// original type when we read the values back in readTFPlan.
		protoChange, err := changeToTfplan(&oc.Change)
		if err != nil {
			return fmt.Errorf("cannot write output value %q: %s", name, err)
		}

		rawPlan.OutputChanges = append(rawPlan.OutputChanges, &planproto.OutputChange{
			Name:      name,
			Change:    protoChange,
			Sensitive: oc.Sensitive,
		})
	}

	src, err := proto.Marshal(rawPlan)
	if err != nil {
		return fmt.Errorf("serialization error: %s", err)
	}

	_, err = w.Write(src)
	if err != nil {
		return fmt.Errorf("failed to write plan to plan file: %s", err)
	}

	return nil
}

func changeToTfplan(change *plans.Change) (*planproto.Change, error) {
	ret := &planproto.Change{}

	before := valueToTfplan(change.Before)
	after := valueToTfplan(change.After)

	switch change.Action {
	case plans.NoOp:
		ret.Action = planproto.Action_NOOP
		ret.Values = []*planproto.DynamicValue{before} // before and after should be identical
	case plans.Create:
		ret.Action = planproto.Action_CREATE
		ret.Values = []*planproto.DynamicValue{after}
	case plans.Read:
		ret.Action = planproto.Action_READ
		ret.Values = []*planproto.DynamicValue{before, after}
	case plans.Update:
		ret.Action = planproto.Action_UPDATE
		ret.Values = []*planproto.DynamicValue{before, after}
	case plans.Replace:
		ret.Action = planproto.Action_REPLACE
		ret.Values = []*planproto.DynamicValue{before, after}
	case plans.Delete:
		ret.Action = planproto.Action_DELETE
		ret.Values = []*planproto.DynamicValue{before}
	default:
		return nil, fmt.Errorf("invalid change action %s", change.Action)
	}

	return ret, nil
}

func valueToTfplan(val plans.DynamicValue) *planproto.DynamicValue {
	if val == nil {
		// protobuf can't represent nil, so we'll represent it as a
		// DynamicValue that has no serializations at all.
		return &planproto.DynamicValue{}
	}
	return &planproto.DynamicValue{
		Msgpack: []byte(val),
	}
}
