package planfile

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io/ioutil"

	"github.com/hashicorp/terraform/plans"
	"github.com/hashicorp/terraform/states/statefile"
)

// Reader is the main type used to read plan files. Create a Reader by calling
// Open.
//
// A plan file is a random-access file format, so methods of Reader must
// be used to access the individual portions of the file for further
// processing.
type Reader struct {
	zip *zip.ReadCloser
}

// Open creates a Reader for the file at the given filename, or returns an
// error if the file doesn't seem to be a planfile.
func Open(filename string) (*Reader, error) {
	r, err := zip.OpenReader(filename)
	if err != nil {
		// To give a better error message, we'll sniff to see if this looks
		// like our old plan format from versions prior to 0.12.
		if b, sErr := ioutil.ReadFile(filename); sErr == nil {
			if bytes.HasPrefix(b, []byte("tfplan")) {
				return nil, fmt.Errorf("the given plan file was created by an earlier version of Terraform; plan files cannot be shared between different Terraform versions")
			}
		}
		return nil, err
	}

	// Sniff to make sure this looks like a plan file, as opposed to any other
	// random zip file the user might have around.
	var planFile *zip.File
	for _, file := range r.File {
		if file.Name == "tfplan" {
			planFile = file
			break
		}
	}
	if planFile == nil {
		return nil, fmt.Errorf("the given file is not a valid plan file")
	}

	// For now, we'll just accept the presence of the file as enough, and
	// wait to validate the version when the caller requests the plan itself.

	return &Reader{
		zip: r,
	}, nil
}

// ReadPlan reads the plan embedded in the plan file.
//
// Errors can be returned for various reasons, including if the plan file
// is not of an appropriate format version, if it was created by a different
// version of Terraform, if it is invalid, etc.
func (r *Reader) ReadPlan() (*plans.Plan, error) {
	var planFile *zip.File
	for _, file := range r.zip.File {
		if file.Name == "tfplan" {
			planFile = file
			break
		}
	}
	if planFile == nil {
		// This should never happen because we checked for this file during
		// Open, but we'll check anyway to be safe.
		return nil, fmt.Errorf("the plan file is invalid")
	}

	pr, err := planFile.Open()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve plan from plan file: %s", err)
	}
	defer pr.Close()

	return readTfplan(pr)
}

// ReadStateFile reads the state file embedded in the plan file.
//
// If the plan file contains no embedded state file, the returned error is
// statefile.ErrNoState.
func (r *Reader) ReadStateFile() (*statefile.File, error) {
	for _, file := range r.zip.File {
		if file.Name == "tfstate" {
			r, err := file.Open()
			if err != nil {
				return nil, fmt.Errorf("failed to extract state from plan file: %s", err)
			}
			return statefile.Read(r)
		}
	}
	return nil, statefile.ErrNoState
}

// Close closes the file, after which no other operations may be performed.
func (r *Reader) Close() error {
	return r.zip.Close()
}
