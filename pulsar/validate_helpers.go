package pulsar

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/hashicorp/go-cty/cty"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/streamnative/pulsarctl/pkg/pulsar/common"
	"github.com/streamnative/pulsarctl/pkg/pulsar/utils"
)

func validateNotBlank(val interface{}, key string) (warns []string, errs []error) {
	v := val.(string)
	if len(strings.Trim(strings.TrimSpace(v), "\"")) == 0 {
		errs = append(errs, fmt.Errorf("%q must not be empty", key))
	}
	return
}

func validateURL(val interface{}, key string) (warns []string, errs []error) {
	v := val.(string)
	_, err := url.ParseRequestURI(strings.Trim(strings.TrimSpace(v), "\""))
	if err != nil {
		errs = append(errs, fmt.Errorf("%q must be a valid url: %w", key, err))
	}
	return
}

func validateGtEq0(val interface{}, key string) (warns []string, errs []error) {
	v := val.(int)
	if v < 0 {
		errs = append(errs, fmt.Errorf("%q must be 0 or more, got: %d", key, v))
	}
	return
}

func validateTopicType(val interface{}, key string) (warns []string, errs []error) {
	v := val.(string)
	_, err := utils.ParseTopicDomain(v)
	if err != nil {
		errs = append(errs, fmt.Errorf("%q must be a valid topic name (got: %s): %w", key, v, err))
	}
	return
}

func validateAuthAction(val interface{}, key string) (warns []string, errs []error) {
	v := val.(string)
	_, err := common.ParseAuthAction(v)
	if err != nil {
		errs = append(errs, fmt.Errorf("%q must be a valid auth action (got: %s): %w", key, v, err))
	}
	return
}

func validateOneOfFactory(validValues []string) schema.SchemaValidateDiagFunc {
	return func(val interface{}, _ cty.Path) diag.Diagnostics {
		v := val.(string)
		for _, validValue := range validValues {
			if v == validValue {
				return nil
			}
		}

		return diag.Diagnostics{diag.Diagnostic{
			Severity: diag.Error,
			Summary:  fmt.Sprintf("must be one of %v and not %v", strings.Join(validValues, ", "), v),
		}}
	}
}
