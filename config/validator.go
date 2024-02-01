// Copyright © 2022 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"reflect"
	"strings"

	"github.com/go-errors/errors"

	"github.com/go-playground/validator/v10"
	"go.uber.org/multierr"
)

// keyStructTag is a tag which contains a field's key.
const (
	keyStructTag = "key"

	// containsOrDefaultTag is a tag for a custom validation function, containsOrDefaultTag.
	containsOrDefaultTag = "contains_or_default"
)

// validate is a singleton instance of the validator.
var validate *validator.Validate

func init() {
	validate = validator.New()
	if err := validate.RegisterValidation(containsOrDefaultTag, containsOrDefault); err != nil {
		panic(err)
	}
}

// Validate validates a struct.
func Validate(data any) error {
	var err error

	validationErr := validate.Struct(data)
	if validationErr != nil {
		if errors.Is(validationErr, (*validator.InvalidValidationError)(nil)) {
			return errors.Errorf("validate struct: %w", validationErr)
		}

		for _, e := range validationErr.(validator.ValidationErrors) {
			fieldName := getFieldKey(data, e.StructField())

			switch e.Tag() {
			case "required":
				err = multierr.Append(err, requiredErr(fieldName))
			case "max":
				err = multierr.Append(err, maxErr(fieldName, e.Param()))
			case containsOrDefaultTag:
				err = multierr.Append(err, containsOrDefaultErr(fieldName, e.Param()))
			case "gte":
				err = multierr.Append(err, gteErr(fieldName, e.Param()))
			case "lte":
				err = multierr.Append(err, lteErr(fieldName, e.Param()))
			}
		}
	}

	return err
}

// requiredErr returns the formatted required error.
func requiredErr(name string) error {
	return errors.Errorf("%s value must be set", name)
}

// maxErr returns the formatted max error.
func maxErr(name, max string) error {
	return errors.Errorf("%q value must be less than or equal to %s", name, max)
}

// containsOrDefaultErr returns the formated contains_or_default error.
func containsOrDefaultErr(name, contains string) error {
	return errors.Errorf("%q value must contains values of these fields: %q", name, contains)
}

// getFieldKey returns a key ("key" tag) for the provided fieldName. If the "key" tag is not present,
// the function will return a fieldName.
func getFieldKey(data any, fieldName string) string {
	// if the data is not pointer or it's nil, return a fieldName.
	val := reflect.ValueOf(data)
	if val.Kind() != reflect.Ptr || val.IsNil() {
		return fieldName
	}

	structField, ok := reflect.TypeOf(data).Elem().FieldByName(fieldName)
	if !ok {
		return fieldName
	}

	fieldKey := structField.Tag.Get(keyStructTag)
	if fieldKey == "" {
		return fieldName
	}

	return fieldKey
}

// containsOrDefault checks whether the string slice contains provided string values or not.
// If the slice is empty the method returns true.
func containsOrDefault(fl validator.FieldLevel) bool {
	inputs, ok := fl.Field().Interface().([]string)
	if !ok {
		return false
	}

	if len(inputs) == 0 {
		return true
	}

	valuesMap := make(map[string]struct{})
	for _, param := range strings.Split(fl.Param(), " ") {
		value, kind, _, ok := fl.GetStructFieldOKAdvanced2(fl.Parent(), param)
		if !ok {
			return false
		}

		switch kind {
		case reflect.String:
			valuesMap[value.String()] = struct{}{}
		case reflect.Slice:
			keys := value.Interface().([]string)
			for i := range keys {
				valuesMap[keys[i]] = struct{}{}
			}
		}
	}

	for _, input := range inputs {
		if _, ok := valuesMap[input]; !ok {
			continue
		}

		delete(valuesMap, input)
	}

	return len(valuesMap) == 0
}

// gteErr returns the formatted gte error.
func gteErr(name, gte string) error {
	return errors.Errorf("%q value must be greater than or equal to %s", name, gte)
}

// lteErr returns the formatted lte error.
func lteErr(name, lte string) error {
	return errors.Errorf("%q value must be less than or equal to %s", name, lte)
}
