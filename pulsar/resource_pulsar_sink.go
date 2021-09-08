// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package pulsar

import (
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/streamnative/pulsarctl/pkg/cli"
	"github.com/streamnative/pulsarctl/pkg/pulsar/utils"
)

func resourcePulsarSink() *schema.Resource {

	return &schema.Resource{
		Create: resourcePulsarSinkCreate,
		Read:   resourcePulsarSinkRead,
		Update: resourcePulsarSinkUpdate,
		Delete: resourcePulsarSinkDelete,
		Exists: resourcePulsarSinkExists,
		Importer: &schema.ResourceImporter{
			State: func(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
				parts := strings.Split(d.Id(), "/")
				if len(parts) != 3 {
					return nil, fmt.Errorf("the import address has to be tenant/namespace/sink")
				}
				d.Set(resourceTenantAttribute, parts[0])
				d.Set(resourceNamespaceAttribute, parts[1])
				d.Set(resourceSinkAttribute, parts[2])
				d.SetId(parts[2])

				err := resourcePulsarSinkRead(d, meta)
				return []*schema.ResourceData{d}, err
			},
		},
		Schema: map[string]*schema.Schema{
			resourceSinkAttribute: {
				Type:        schema.TypeString,
				Required:    true,
				Description: descriptions[resourceSinkAttribute],
			},
			resourceTenantAttribute: {
				Type:        schema.TypeString,
				Required:    true,
				Description: descriptions[resourceTenantAttribute],
			},
			resourceNamespaceAttribute: {
				Type:        schema.TypeString,
				Required:    true,
				Description: descriptions[resourceNamespaceAttribute],
			},
			resourceArchiveAttribute: {
				Type:        schema.TypeString,
				Optional:    true,
				Description: descriptions[resourceArchiveAttribute],
			},
			resourceParallelismAttribute: {
				Type:        schema.TypeInt,
				Optional:    true,
				Default:     1,
				Description: descriptions[resourceParallelismAttribute],
			},
			resourceProcessingGuaranteesAttribute: {
				Type:             schema.TypeString,
				Optional:         true,
				ForceNew:         true,
				Default:          processingGuaranteeEffectivelyOnce,
				ValidateDiagFunc: validateOneOfFactory(processingGuarantees),
				Description:      descriptions[resourceParallelismAttribute],
			},
			resourceRetainOrderingAttribute: {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     true,
				Description: descriptions[resourceRetainOrderingAttribute],
			},
			resourceInputsAttribute: {
				Type:        schema.TypeList,
				Required:    true,
				MinItems:    1,
				ForceNew:    true,
				Description: descriptions[resourceInputsAttribute],
				Elem: &schema.Schema{
					Type:         schema.TypeString,
					ValidateFunc: validateNotBlank,
				},
			},
			resourceConfigsAttribute: {
				Type:        schema.TypeMap,
				Optional:    true,
				Description: descriptions[resourceConfigsAttribute],
			},
			resourceCustomRuntimeOptionsAttribute: {
				Type:        schema.TypeString,
				Optional:    true,
				Description: descriptions[resourceCustomRuntimeOptionsAttribute],
			},
		},
	}
}

func resourcePulsarSinkCreate(d *schema.ResourceData, meta interface{}) error {
	client := getClientV3FromMeta(meta).Sinks()

	ok, err := resourcePulsarSinkExists(d, meta)
	if err != nil {
		return err
	}

	if ok {
		return resourcePulsarSinkRead(d, meta)
	}

	sink := d.Get(resourceSinkAttribute).(string)
	archiveFile := d.Get(resourceArchiveAttribute).(string)
	isUrl := isArchiveUrl(archiveFile)

	var createSink func(*utils.SinkConfig, string) error
	if isUrl {
		createSink = client.CreateSinkWithURL
	} else {
		createSink = client.CreateSink
	}
	sinkConfig := marshalSinkData(d)

	if err := createSink(sinkConfig, archiveFile); err != nil {
		return fmt.Errorf("ERROR_CREATE_SINK: %w", err)
	}

	_ = d.Set("sink", sink)
	return resourcePulsarSinkRead(d, meta)
}

func resourcePulsarSinkRead(d *schema.ResourceData, meta interface{}) error {
	client := getClientV3FromMeta(meta).Sinks()

	sinkName := d.Get(resourceSinkAttribute).(string)
	tenant := d.Get(resourceTenantAttribute).(string)
	namespace := d.Get(resourceNamespaceAttribute).(string)

	sinkConfig, err := client.GetSink(tenant, namespace, sinkName)
	if err != nil {
		return fmt.Errorf("ERROR_READ_SINK_DATA: %w", err)
	}

	unmarshalSinkData(d, &sinkConfig)

	d.SetId(sinkName)

	return nil
}

func resourcePulsarSinkUpdate(d *schema.ResourceData, meta interface{}) error {
	client := getClientV3FromMeta(meta).Sinks()

	sinkConfig := marshalSinkData(d)
	archiveFile := d.Get(resourceArchiveAttribute).(string)
	isUrl := isArchiveUrl(archiveFile)

	var updateSink func(*utils.SinkConfig, string, *utils.UpdateOptions) error
	if isUrl {
		updateSink = client.UpdateSinkWithURL
	} else {
		updateSink = client.UpdateSink
	}

	if err := updateSink(sinkConfig, archiveFile, &utils.UpdateOptions{UpdateAuthData: true}); err != nil {
		return fmt.Errorf("ERROR_UPDATE_SINK: %w", err)
	}

	// d.SetId(cluster)

	return nil
}

func resourcePulsarSinkDelete(d *schema.ResourceData, meta interface{}) error {
	client := getClientV3FromMeta(meta).Sinks()

	sinkName := d.Get(resourceSinkAttribute).(string)
	tenant := d.Get(resourceTenantAttribute).(string)
	namespace := d.Get(resourceNamespaceAttribute).(string)

	if err := client.DeleteSink(tenant, namespace, sinkName); err != nil {
		return fmt.Errorf("ERROR_DELETE_SINK: %w", err)
	}

	_ = d.Set(resourceSinkAttribute, "")

	return nil
}

func resourcePulsarSinkExists(d *schema.ResourceData, meta interface{}) (bool, error) {
	client := getClientV3FromMeta(meta).Sinks()

	sinkName := d.Get(resourceSinkAttribute).(string)
	tenant := d.Get(resourceTenantAttribute).(string)
	namespace := d.Get(resourceNamespaceAttribute).(string)

	if _, err := client.GetSink(tenant, namespace, sinkName); err != nil {
		if cliErr, ok := err.(cli.Error); ok && cliErr.Code == 404 {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func marshalSinkData(input *schema.ResourceData) *utils.SinkConfig {
	var sinkConfig utils.SinkConfig

	sinkConfig.Tenant = input.Get(resourceTenantAttribute).(string)
	sinkConfig.Namespace = input.Get(resourceNamespaceAttribute).(string)
	sinkConfig.Name = input.Get(resourceSinkAttribute).(string)

	sinkConfig.Archive = input.Get(resourceArchiveAttribute).(string)
	sinkConfig.Parallelism = input.Get(resourceParallelismAttribute).(int)
	sinkConfig.Inputs = handleHCLArrayV2(input.Get(resourceInputsAttribute).([]interface{}))
	sinkConfig.Configs = input.Get(resourceConfigsAttribute).(map[string]interface{})
	sinkConfig.RetainOrdering = input.Get(resourceRetainOrderingAttribute).(bool)
	sinkConfig.ProcessingGuarantees = input.Get(resourceProcessingGuaranteesAttribute).(string)
	sinkConfig.CustomRuntimeOptions = input.Get(resourceCustomRuntimeOptionsAttribute).(string)

	return &sinkConfig
}

func unmarshalSinkData(d *schema.ResourceData, sinkConfig *utils.SinkConfig) {
	// IMPORTANT: Do not read Archive and Inputs as they never return a value from the API
	// And terraform will see it as a drift.
	// Let terraform deal with the change if the user changed these variables.

	d.Set(resourceTenantAttribute, sinkConfig.Tenant)
	d.Set(resourceNamespaceAttribute, sinkConfig.Namespace)
	d.Set(resourceSinkAttribute, sinkConfig.Name)

	d.Set(resourceParallelismAttribute, sinkConfig.Parallelism)

	d.Set(resourceConfigsAttribute, sinkConfig.Configs)
	d.Set(resourceRetainOrderingAttribute, sinkConfig.RetainOrdering)
	d.Set(resourceProcessingGuaranteesAttribute, sinkConfig.ProcessingGuarantees)
	d.Set(resourceCustomRuntimeOptionsAttribute, sinkConfig.CustomRuntimeOptions)
}

func isArchiveUrl(archiveFile string) bool {
	for _, scheme := range supportedArchiveUrlSchemes {
		if strings.HasPrefix(archiveFile, scheme) {
			return true
		}
	}

	return false
}
