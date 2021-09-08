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
	"net/url"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/streamnative/pulsarctl/pkg/cli"
	"github.com/streamnative/pulsarctl/pkg/pulsar"
	"github.com/streamnative/pulsarctl/pkg/pulsar/utils"
)

func resourcePulsarFunction() *schema.Resource {

	return &schema.Resource{
		Create: resourcePulsarFunctionCreate,
		Read:   resourcePulsarFunctionRead,
		Update: resourcePulsarFunctionUpdate,
		Delete: resourcePulsarFunctionDelete,
		Exists: resourcePulsarFunctionExists,
		Importer: &schema.ResourceImporter{
			State: func(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
				_ = d.Set(resourceFunctionAttribute, d.Id())
				err := resourcePulsarFunctionRead(d, meta)
				return []*schema.ResourceData{d}, err
			},
		},
		Schema: map[string]*schema.Schema{
			resourceFunctionAttribute: {
				Type:        schema.TypeString,
				Required:    true,
				Description: descriptions[resourceFunctionAttribute],
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
			resourceGoAttribute: {
				Type:        schema.TypeString,
				Optional:    true,
				Description: descriptions[resourceGoAttribute],
			},
			resourceJarAttribute: {
				Type:        schema.TypeString,
				Optional:    true,
				Description: descriptions[resourceJarAttribute],
			},
			resourcePyAttribute: {
				Type:        schema.TypeString,
				Optional:    true,
				Description: descriptions[resourcePyAttribute],
			},
		},
	}
}

func resourcePulsarFunctionCreate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(pulsar.Client).Functions()

	ok, err := resourcePulsarFunctionExists(d, meta)
	if err != nil {
		return err
	}

	if ok {
		return resourcePulsarFunctionRead(d, meta)
	}

	function := d.Get(resourceFunctionAttribute).(string)
	codeFile, isUrl, err := getFunctionCode(d)
	if err != nil {
		return err
	}
	var createFunction func(*utils.FunctionConfig, string) error
	if isUrl {
		createFunction = client.CreateFuncWithURL
	} else {
		createFunction = client.CreateFunc
	}
	functionConfig := unmarshalFunctionData(d)

	if err := createFunction(functionConfig, codeFile); err != nil {
		return fmt.Errorf("ERROR_CREATE_FUNCTION: %w", err)
	}

	_ = d.Set("function", function)
	return resourcePulsarFunctionRead(d, meta)
}

func resourcePulsarFunctionRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(pulsar.Client).Functions()

	functionName := d.Get(resourceFunctionAttribute).(string)
	tenant := d.Get(resourceTenantAttribute).(string)
	namespace := d.Get(resourceNamespaceAttribute).(string)

	_, err := client.GetFunction(tenant, namespace, functionName)
	if err != nil {
		return fmt.Errorf("ERROR_READ_FUNCTION_DATA: %w", err)
	}

	d.SetId(functionName)

	return nil
}

func resourcePulsarFunctionUpdate(d *schema.ResourceData, meta interface{}) error {
	panic("not implemented")
	// client := meta.(pulsar.Client).Clusters()

	// clusterDataSet := d.Get("cluster_data").(*schema.Set)
	// cluster := d.Get("cluster").(string)

	// clusterData := unmarshalClusterData(clusterDataSet)
	// clusterData.Name = cluster

	// if err := client.Update(*clusterData); err != nil {
	// 	return fmt.Errorf("ERROR_UPDATE_CLUSTER_DATA: %w", err)
	// }

	// _ = d.Set("cluster_data", clusterDataSet)
	// d.SetId(cluster)

	return nil
}

func resourcePulsarFunctionDelete(d *schema.ResourceData, meta interface{}) error {
	client := meta.(pulsar.Client).Functions()

	functionName := d.Get(resourceFunctionAttribute).(string)
	tenant := d.Get(resourceTenantAttribute).(string)
	namespace := d.Get(resourceNameAttribute).(string)

	if err := client.DeleteFunction(tenant, namespace, functionName); err != nil {
		return fmt.Errorf("ERROR_DELETE_FUNCTION: %w", err)
	}

	_ = d.Set(resourceFunctionAttribute, "")

	return nil
}

func resourcePulsarFunctionExists(d *schema.ResourceData, meta interface{}) (bool, error) {
	client := meta.(pulsar.Client).Functions()

	functionName := d.Get(resourceFunctionAttribute).(string)
	tenant := d.Get(resourceTenantAttribute).(string)
	namespace := d.Get(resourceNamespaceAttribute).(string)

	if _, err := client.GetFunction(tenant, namespace, functionName); err != nil {
		if cliErr, ok := err.(cli.Error); ok && cliErr.Code == 404 {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func unmarshalFunctionData(input *schema.ResourceData) *utils.FunctionConfig {
	var functionData utils.FunctionConfig

	functionData.Tenant = input.Get(resourceTenantAttribute).(string)
	functionData.Namespace = input.Get(resourceNamespaceAttribute).(string)
	functionData.Name = input.Get(resourceFunctionAttribute).(string)

	goFile := input.Get(resourceGoAttribute).(string)
	functionData.Go = &goFile
	jarFile := input.Get(resourceJarAttribute).(string)
	functionData.Jar = &jarFile
	pyFile := input.Get(resourcePyAttribute).(string)
	functionData.Py = &pyFile

	return &functionData
}

func getFunctionCode(input *schema.ResourceData) (string, bool, error) {
	var codeFile string

	goFile := input.Get(resourceGoAttribute).(string)
	jarFile := input.Get(resourceJarAttribute).(string)
	pyFile := input.Get(resourcePyAttribute).(string)

	codePresent := false
	isUrl := false

	for _, maybeFile := range []string{goFile, jarFile, pyFile} {
		if codePresent && maybeFile != "" {
			return "", false, fmt.Errorf("ERROR_MULTIPLE_CODE_FILES: Multiple code files provided")
		}
		if maybeFile != "" {
			codePresent = true
			codeFile = maybeFile
			_, err := url.Parse(codeFile)
			isUrl = err == nil
		}
	}

	if !codePresent {
		return "", false, fmt.Errorf("ERROR_NO_CODE_FILE: Either go, jar, or py should be filled")
	}

	return codeFile, isUrl, nil
}
