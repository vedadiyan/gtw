/*
	Extracted from github.com/vedadiyan/goal/pkg/di
	DO NOT EDIT
*/

package di

import (
	"fmt"
)

func objectNotFoundError(name string) error {
	return fmt.Errorf("an object of type `%s` has not been registered", name)
}

func objectAlreadyExistsError(name string) error {
	return fmt.Errorf("an object of type `%s` has already been registered", name)
}

func invalidCastError(name string) error {
	return fmt.Errorf("the registered object cannot be cast to `%s`", name)
}

func missingRequiredParameter(name string) error {
	return fmt.Errorf("the `%s` parameter is required for scoped services", name)
}
