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
	"io/ioutil"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/streamnative/pulsarctl/pkg/pulsar"
)

func init() {
	initTestWebServiceURL()

	resource.AddTestSweepers("pulsar_function", &resource.Sweeper{
		Name: "pulsar_function",
		F:    testSweepFunction,
		Dependencies: []string{
			"pulsar_cluster",
			"pulsar_tenant",
		},
	})
}

func testSweepFunction(url string) error {

	client, err := sharedClient(url)
	if err != nil {
		return fmt.Errorf("ERROR_GETTING_PULSAR_CLIENT: %w", err)
	}

	conn := client.(pulsar.Client)

	tenants, err := conn.Tenants().List()
	if err != nil {
		return fmt.Errorf("ERROR_GETTING_TENANTS: %w", err)
	}

	for _, t := range tenants {
		namespaces, err := conn.Namespaces().GetNamespaces(t)
		if err != nil {
			return fmt.Errorf("ERROR_GETTING_NAMESPACE_LIST: %w", err)
		}

		for _, ns := range namespaces {
			functions, err := conn.Functions().GetFunctions(t, ns)
			if err != nil {
				return fmt.Errorf("ERROR_GETTING_FUNCTIONS: %w", err)
			}
			for _, function := range functions {
				if err := conn.Functions().DeleteFunction(t, ns, function); err != nil {
					return fmt.Errorf("ERROR_DELETING_FUNCTION: %w", err)
				}
			}
		}
	}

	return nil
}

func TestFunction(t *testing.T) {

	resourceName := "pulsar_function.test"
	cluster := acctest.RandString(10)
	tenant := acctest.RandString(10)
	namespace := acctest.RandString(10)
	function := acctest.RandString(10)

	resource.Test(t, resource.TestCase{
		PreCheck:      func() { testAccPreCheck(t) },
		Providers:     testAccProviders,
		IDRefreshName: resourceName,
		CheckDestroy:  testPulsarFunctionDestroy,
		Steps: []resource.TestStep{
			{
				Config: testPulsarFunction(testWebServiceURL, cluster, tenant, namespace, function),
				Check: resource.ComposeTestCheckFunc(
					testPulsarFunctionExists(resourceName),
				),
			},
		},
	})
}

// func TestFunctionWithUpdate(t *testing.T) {

// 	resourceName := "pulsar_function.test"
// 	cName := acctest.RandString(10)
// 	tName := acctest.RandString(10)
// 	nsName := acctest.RandString(10)

// 	resource.Test(t, resource.TestCase{
// 		PreCheck:      func() { testAccPreCheck(t) },
// 		Providers:     testAccProviders,
// 		IDRefreshName: resourceName,
// 		CheckDestroy:  testPulsarFunctionDestroy,
// 		Steps: []resource.TestStep{
// 			{
// 				Config: testPulsarFunctionWithoutOptionals(testWebServiceURL, cName, tName, nsName),
// 				Check: resource.ComposeTestCheckFunc(
// 					testPulsarFunctionExists(resourceName),
// 					resource.TestCheckResourceAttr(resourceName, "dispatch_rate.#", "0"),
// 					resource.TestCheckResourceAttr(resourceName, "retention_policies.#", "0"),
// 					resource.TestCheckResourceAttr(resourceName, "function_config.#", "0"),
// 					resource.TestCheckNoResourceAttr(resourceName, "enable_deduplication"),
// 					resource.TestCheckNoResourceAttr(resourceName, "permission_grant"),
// 				),
// 			},
// 			{
// 				Config: testPulsarFunction(testWebServiceURL, cName, tName, nsName),
// 				Check: resource.ComposeTestCheckFunc(
// 					testPulsarFunctionExists(resourceName),
// 					resource.TestCheckResourceAttr(resourceName, "dispatch_rate.#", "1"),
// 					resource.TestCheckResourceAttr(resourceName, "retention_policies.#", "1"),
// 					resource.TestCheckResourceAttr(resourceName, "function_config.#", "1"),
// 					resource.TestCheckResourceAttr(resourceName, "enable_deduplication", "true"),
// 					resource.TestCheckResourceAttr(resourceName, "permission_grant.#", "2"),
// 					resource.TestCheckResourceAttr(resourceName, "permission_grant.0.role", "some-role-1"),
// 					resource.TestCheckResourceAttr(resourceName, "permission_grant.0.actions.#", "3"),
// 					resource.TestCheckResourceAttr(resourceName, "permission_grant.0.actions.0", "consume"),
// 					resource.TestCheckResourceAttr(resourceName, "permission_grant.0.actions.1", "functions"),
// 					resource.TestCheckResourceAttr(resourceName, "permission_grant.0.actions.2", "produce"),
// 					resource.TestCheckResourceAttr(resourceName, "permission_grant.1.role", "some-role-2"),
// 					resource.TestCheckResourceAttr(resourceName, "permission_grant.1.actions.#", "2"),
// 					resource.TestCheckResourceAttr(resourceName, "permission_grant.1.actions.0", "consume"),
// 					resource.TestCheckResourceAttr(resourceName, "permission_grant.1.actions.1", "produce"),
// 				),
// 			},
// 		},
// 	})
// }

// func TestFunctionWithUndefinedOptionalsUpdate(t *testing.T) {

// 	resourceName := "pulsar_function.test"
// 	cName := acctest.RandString(10)
// 	tName := acctest.RandString(10)
// 	nsName := acctest.RandString(10)

// 	resource.Test(t, resource.TestCase{
// 		PreCheck:      func() { testAccPreCheck(t) },
// 		Providers:     testAccProviders,
// 		IDRefreshName: resourceName,
// 		CheckDestroy:  testPulsarFunctionDestroy,
// 		Steps: []resource.TestStep{
// 			{
// 				Config: testPulsarFunctionWithoutOptionals(testWebServiceURL, cName, tName, nsName),
// 				Check: resource.ComposeTestCheckFunc(
// 					testPulsarFunctionExists(resourceName),
// 					resource.TestCheckResourceAttr(resourceName, "dispatch_rate.#", "0"),
// 					resource.TestCheckResourceAttr(resourceName, "retention_policies.#", "0"),
// 					resource.TestCheckResourceAttr(resourceName, "backlog_quota.#", "0"),
// 					resource.TestCheckResourceAttr(resourceName, "function_config.#", "0"),
// 					resource.TestCheckNoResourceAttr(resourceName, "enable_deduplication"),
// 					resource.TestCheckNoResourceAttr(resourceName, "permission_grant"),
// 				),
// 			},
// 			{
// 				Config: testPulsarFunctionWithUndefinedOptionalsInNsConf(testWebServiceURL, cName, tName, nsName),
// 				Check: resource.ComposeTestCheckFunc(
// 					testPulsarFunctionExists(resourceName),
// 					resource.TestCheckResourceAttr(resourceName, "dispatch_rate.#", "0"),
// 					resource.TestCheckResourceAttr(resourceName, "retention_policies.#", "0"),
// 					resource.TestCheckResourceAttr(resourceName, "backlog_quota.#", "0"),
// 					resource.TestCheckResourceAttr(resourceName, "function_config.#", "1"),
// 					resource.TestCheckNoResourceAttr(resourceName, "enable_deduplication"),
// 					resource.TestCheckNoResourceAttr(resourceName, "permission_grant"),
// 				),
// 				ExpectNonEmptyPlan: true,
// 			},
// 		},
// 	})
// }
// func TestImportExistingFunction(t *testing.T) {
// 	tname := "public"
// 	ns := acctest.RandString(10)

// 	id := tname + "/" + ns

// 	resource.Test(t, resource.TestCase{
// 		PreCheck: func() {
// 			testAccPreCheck(t)
// 			createFunction(t, id)
// 		},
// 		CheckDestroy: testPulsarFunctionDestroy,
// 		Providers:    testAccProviders,
// 		Steps: []resource.TestStep{
// 			{
// 				ResourceName:     "pulsar_function.test",
// 				ImportState:      true,
// 				Config:           testPulsarExistingFunctionWithoutOptionals(testWebServiceURL, ns),
// 				ImportStateId:    id,
// 				ImportStateCheck: testFunctionImported(),
// 			},
// 		},
// 	})
// }

// func createFunction(t *testing.T, tenant, namespace, name string) {
// 	client, err := sharedClient(testWebServiceURL)
// 	if err != nil {
// 		t.Fatalf("ERROR_GETTING_PULSAR_CLIENT: %v", err)
// 	}

// 	conn := client.(pulsar.Client)
// 	if err = conn.Functions().CreateFuncWithURL(); err != nil {
// 		t.Fatalf("ERROR_CREATING_TEST_FUNCTION: %v", err)
// 	}
// }

// //nolint:unparam
func testPulsarFunctionExists(function string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		panic("Not implemented")
		// rs, ok := s.RootModule().Resources[function]
		// if !ok {
		// 	return fmt.Errorf("NOT_FOUND: %s", function)
		// }

		// client := testAccProvider.Meta().(pulsar.Client).Functions()

		// if rs.Primary.ID == "" || !strings.Contains(rs.Primary.ID, "/") {
		// 	return fmt.Errorf(`ERROR_NAMESPACE_ID_INVALID: "%s"`, rs.Primary.ID)
		// }

		// // id is the full path of the function, tenant-name/function-name
		// // split would give us [tenant-name, function-name]
		// nsParts := strings.Split(rs.Primary.ID, "/")

		// nsList, err := client.GetFunctions(nsParts[0])
		// if err != nil {
		// 	return fmt.Errorf("ERROR_READ_NAMESPACE_DATA: %w\n input data: %s", err, nsParts[0])
		// }

		// for _, ns := range nsList {

		// 	if ns == rs.Primary.ID {
		// 		return nil
		// 	}
		// }

		// return fmt.Errorf(`ERROR_RESOURCE_NAMESPACE_DOES_NOT_EXISTS: "%s"`, function)
	}
}

// func testFunctionImported() resource.ImportStateCheckFunc {
// 	return func(s []*terraform.InstanceState) error {
// 		if len(s) != 1 {
// 			return fmt.Errorf("expected %d states, got %d: %#v", 1, len(s), s)
// 		}

// 		if len(s[0].Attributes) != 10 {
// 			return fmt.Errorf("expected %d attrs, got %d: %#v", 10, len(s[0].Attributes), s[0].Attributes)
// 		}

// 		return nil
// 	}
// }

func testPulsarFunctionDestroy(s *terraform.State) error {
	// client := testAccProvider.Meta().(pulsar.Client).Functions()
	panic("implement me")
	for _, rs := range s.RootModule().Resources {
		if rs.Type != "pulsar_function" {
			continue
		}

		// id is the full path of the function, in the format of tenant-name/function-name
		// split would give us [tenant-name, function-name]
		if rs.Primary.ID == "" || !strings.Contains(rs.Primary.ID, "/") {
			return fmt.Errorf(`ERROR_INVALID_RESOURCE_ID: "%s"`, rs.Primary.ID)
		}

		// nsParts := strings.Split(rs.Primary.ID, "/")

		// nsList, err := client.GetFunctions(nsParts[0])
		// if err != nil {
		// 	return nil
		// }

		// for _, ns := range nsList {
		// 	if ns == rs.Primary.ID {
		// 		return fmt.Errorf("ERROR_RESOURCE_NAMESPACE_STILL_EXISTS: %s", ns)
		// 	}
		// }
	}

	return nil
}

func testPulsarFunction(wsURL, cluster, tenant, namespace, function string) string {
	goFile, _ := ioutil.TempFile("", "file.go")

	return fmt.Sprintf(`
provider "pulsar" {
  web_service_url = "%s"
}

resource "pulsar_cluster" "test_cluster" {
  cluster = "%s"

  cluster_data {
    web_service_url    = "http://localhost:8080"
    broker_service_url = "http://localhost:6050"
    peer_clusters      = ["standalone"]
  }
}

resource "pulsar_tenant" "test_tenant" {
  tenant           = "%s"
  allowed_clusters = [pulsar_cluster.test_cluster.cluster, "standalone"]
}

resource "pulsar_namespace" "test_namespace" {
  tenant = pulsar_tenant.test_tenant.tenant
  namespace   = "%s"
}

resource "pulsar_function" "test" {
  tenant    = pulsar_tenant.test_tenant.tenant
  namespace = pulsar_namespace.test_namespace.namespace
  function  = "%s"
  go        = "%s"
}
`, wsURL, cluster, tenant, namespace, function, goFile.Name())
}

// func testPulsarFunctionWithUndefinedOptionalsInNsConf(wsURL, cluster, tenant, ns string) string {
// 	return fmt.Sprintf(`
// provider "pulsar" {
//   web_service_url = "%s"
// }

// resource "pulsar_cluster" "test_cluster" {
//   cluster = "%s"

//   cluster_data {
//     web_service_url    = "http://localhost:8080"
//     broker_service_url = "http://localhost:6050"
//     peer_clusters      = ["standalone"]
//   }

// }

// resource "pulsar_tenant" "test_tenant" {
//   tenant           = "%s"
//   allowed_clusters = [pulsar_cluster.test_cluster.cluster, "standalone"]
// }

// resource "pulsar_function" "test" {
//   tenant    = pulsar_tenant.test_tenant.tenant
//   function = "%s"

//   function_config {
//     anti_affinity                  = "anti-aff"
//     max_producers_per_topic        = "50"
//   }

// }
// `, wsURL, cluster, tenant, ns)
// }

// func testPulsarExistingFunctionWithoutOptionals(wsURL, ns string) string {
// 	return fmt.Sprintf(`
// provider "pulsar" {
//   web_service_url = "%s"
// }

// resource "pulsar_function" "test" {
//   tenant    = "public"
//   function = "%s"
// }
// `, wsURL, ns)
// }

// func testPulsarFunctionWithPermissionGrants(wsURL, cluster, tenant, ns string, permissionGrants string) string {
// 	return fmt.Sprintf(`
// provider "pulsar" {
//   web_service_url = "%s"
// }

// resource "pulsar_cluster" "test_cluster" {
//   cluster = "%s"

//   cluster_data {
//     web_service_url    = "http://localhost:8080"
//     broker_service_url = "http://localhost:6050"
//     peer_clusters      = ["standalone"]
//   }

// }

// resource "pulsar_tenant" "test_tenant" {
//   tenant           = "%s"
//   allowed_clusters = [pulsar_cluster.test_cluster.cluster, "standalone"]
// }

// resource "pulsar_function" "test" {
//   tenant    = pulsar_tenant.test_tenant.tenant
// 	function = "%s"

// 	%s
// }
// `, wsURL, cluster, tenant, ns, permissionGrants)
// }
