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
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/streamnative/pulsarctl/pkg/pulsar"
	"github.com/streamnative/pulsarctl/pkg/pulsar/utils"
)

const (
	githubArchiveFile = "https://github.com/streamnative/pulsar-io-cloud-storage/releases/download/v2.8.1.0/pulsar-io-cloud-storage-2.8.1.0.nar"
)

func init() {
	initTestWebServiceURL()

	resource.AddTestSweepers("pulsar_sink", &resource.Sweeper{
		Name: "pulsar_sink",
		F:    testSweepSink,
		Dependencies: []string{
			"pulsar_cluster",
			"pulsar_tenant",
		},
	})
}

func testSweepSink(url string) error {

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
			sinks, err := conn.Sinks().ListSinks(t, ns)
			if err != nil {
				return fmt.Errorf("ERROR_GETTING_FUNCTIONS: %w", err)
			}
			for _, sink := range sinks {
				if err := conn.Sinks().DeleteSink(t, ns, sink); err != nil {
					return fmt.Errorf("ERROR_DELETING_FUNCTION: %w", err)
				}
			}
		}
	}

	return nil
}

func TestSink(t *testing.T) {

	resourceName := "pulsar_sink.test"
	cluster := fmt.Sprintf("cluster-%s", acctest.RandString(10))
	tenant := fmt.Sprintf("tenant-%s", acctest.RandString(10))
	namespace := fmt.Sprintf("namespace-%s", acctest.RandString(10))
	topic := fmt.Sprintf("topic-%s", acctest.RandString(10))
	sink := fmt.Sprintf("sink-%s", acctest.RandString(10))

	resource.Test(t, resource.TestCase{
		PreCheck:      func() { testAccPreCheck(t) },
		Providers:     testAccProviders,
		IDRefreshName: resourceName,
		CheckDestroy:  testPulsarSinkDestroy,
		Steps: []resource.TestStep{
			{
				Config: testPulsarSink(testWebServiceURL, cluster, tenant, namespace, topic, sink),
				Check: resource.ComposeTestCheckFunc(
					testPulsarSinkExists(resourceName),
				),
			},
		},
	})
}

// func TestSinkWithUpdate(t *testing.T) {

// 	resourceName := "pulsar_sink.test"
// 	cName := acctest.RandString(10)
// 	tName := acctest.RandString(10)
// 	nsName := acctest.RandString(10)

// 	resource.Test(t, resource.TestCase{
// 		PreCheck:      func() { testAccPreCheck(t) },
// 		Providers:     testAccProviders,
// 		IDRefreshName: resourceName,
// 		CheckDestroy:  testPulsarSinkDestroy,
// 		Steps: []resource.TestStep{
// 			{
// 				Config: testPulsarSinkWithoutOptionals(testWebServiceURL, cName, tName, nsName),
// 				Check: resource.ComposeTestCheckFunc(
// 					testPulsarSinkExists(resourceName),
// 					resource.TestCheckResourceAttr(resourceName, "dispatch_rate.#", "0"),
// 					resource.TestCheckResourceAttr(resourceName, "retention_policies.#", "0"),
// 					resource.TestCheckResourceAttr(resourceName, "sink_config.#", "0"),
// 					resource.TestCheckNoResourceAttr(resourceName, "enable_deduplication"),
// 					resource.TestCheckNoResourceAttr(resourceName, "permission_grant"),
// 				),
// 			},
// 			{
// 				Config: testPulsarSink(testWebServiceURL, cName, tName, nsName),
// 				Check: resource.ComposeTestCheckFunc(
// 					testPulsarSinkExists(resourceName),
// 					resource.TestCheckResourceAttr(resourceName, "dispatch_rate.#", "1"),
// 					resource.TestCheckResourceAttr(resourceName, "retention_policies.#", "1"),
// 					resource.TestCheckResourceAttr(resourceName, "sink_config.#", "1"),
// 					resource.TestCheckResourceAttr(resourceName, "enable_deduplication", "true"),
// 					resource.TestCheckResourceAttr(resourceName, "permission_grant.#", "2"),
// 					resource.TestCheckResourceAttr(resourceName, "permission_grant.0.role", "some-role-1"),
// 					resource.TestCheckResourceAttr(resourceName, "permission_grant.0.actions.#", "3"),
// 					resource.TestCheckResourceAttr(resourceName, "permission_grant.0.actions.0", "consume"),
// 					resource.TestCheckResourceAttr(resourceName, "permission_grant.0.actions.1", "sinks"),
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

// func TestSinkWithUndefinedOptionalsUpdate(t *testing.T) {

// 	resourceName := "pulsar_sink.test"
// 	cName := acctest.RandString(10)
// 	tName := acctest.RandString(10)
// 	nsName := acctest.RandString(10)

// 	resource.Test(t, resource.TestCase{
// 		PreCheck:      func() { testAccPreCheck(t) },
// 		Providers:     testAccProviders,
// 		IDRefreshName: resourceName,
// 		CheckDestroy:  testPulsarSinkDestroy,
// 		Steps: []resource.TestStep{
// 			{
// 				Config: testPulsarSinkWithoutOptionals(testWebServiceURL, cName, tName, nsName),
// 				Check: resource.ComposeTestCheckFunc(
// 					testPulsarSinkExists(resourceName),
// 					resource.TestCheckResourceAttr(resourceName, "dispatch_rate.#", "0"),
// 					resource.TestCheckResourceAttr(resourceName, "retention_policies.#", "0"),
// 					resource.TestCheckResourceAttr(resourceName, "backlog_quota.#", "0"),
// 					resource.TestCheckResourceAttr(resourceName, "sink_config.#", "0"),
// 					resource.TestCheckNoResourceAttr(resourceName, "enable_deduplication"),
// 					resource.TestCheckNoResourceAttr(resourceName, "permission_grant"),
// 				),
// 			},
// 			{
// 				Config: testPulsarSinkWithUndefinedOptionalsInNsConf(testWebServiceURL, cName, tName, nsName),
// 				Check: resource.ComposeTestCheckFunc(
// 					testPulsarSinkExists(resourceName),
// 					resource.TestCheckResourceAttr(resourceName, "dispatch_rate.#", "0"),
// 					resource.TestCheckResourceAttr(resourceName, "retention_policies.#", "0"),
// 					resource.TestCheckResourceAttr(resourceName, "backlog_quota.#", "0"),
// 					resource.TestCheckResourceAttr(resourceName, "sink_config.#", "1"),
// 					resource.TestCheckNoResourceAttr(resourceName, "enable_deduplication"),
// 					resource.TestCheckNoResourceAttr(resourceName, "permission_grant"),
// 				),
// 				ExpectNonEmptyPlan: true,
// 			},
// 		},
// 	})
// }

func TestImportExistingSink(t *testing.T) {
	tenant := "public"
	namespace := "default"
	topic := fmt.Sprintf("topic-%s", acctest.RandString(10))
	sink := fmt.Sprintf("sink-%s", acctest.RandString(10))

	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			createSink(t, tenant, namespace, topic, sink)
		},
		CheckDestroy: testPulsarSinkDestroy,
		Providers:    testAccProviders,
		Steps: []resource.TestStep{
			{
				ResourceName:     "pulsar_sink.test",
				ImportState:      true,
				Config:           testPulsarExistingSinkWithoutOptionals(testWebServiceURL, topic, sink),
				ImportStateId:    fmt.Sprintf("%s/%s/%s", tenant, namespace, sink),
				ImportStateCheck: testSinkImported(),
			},
		},
	})
}

func createSink(t *testing.T, tenant, namespace, topic, sink string) {
	createTopic(t, fmt.Sprintf("persistent://%s/%s/%s", tenant, namespace, topic), 1)
	client, err := sharedClientV3(testWebServiceURL)
	if err != nil {
		t.Fatalf("ERROR_GETTING_PULSAR_CLIENT: %v", err)
	}

	data := &utils.SinkConfig{
		Tenant:      tenant,
		Namespace:   namespace,
		Name:        sink,
		Archive:     githubArchiveFile,
		Parallelism: 1,
		Inputs:      []string{topic},
	}

	conn := client.(pulsar.Client)
	if err = conn.Sinks().CreateSinkWithURL(data, githubArchiveFile); err != nil {
		t.Fatalf("ERROR_CREATING_TEST_FUNCTION: %v", err)
	}
}

// //nolint:unparam
func testPulsarSinkExists(sink string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[sink]
		if !ok {
			return fmt.Errorf("NOT_FOUND: %s", sink)
		}

		client := getClientV3FromMeta(testAccProvider.Meta()).Sinks()
		sinkExists, err := testPulsarSinkExistsFromInstanceState(client, rs.Primary)
		if err != nil {
			return err
		}

		if !sinkExists {
			return fmt.Errorf(`ERROR_RESOURCE_SINK_DOES_NOT_EXISTS: "%s"`, sink)
		}
		return nil
	}
}

func testSinkImported() resource.ImportStateCheckFunc {
	return func(s []*terraform.InstanceState) error {
		if len(s) != 1 {
			return fmt.Errorf("expected %d states, got %d: %#v", 1, len(s), s)
		}

		if len(s[0].Attributes) != 10 {
			return fmt.Errorf("expected %d attrs, got %d: %#v", 10, len(s[0].Attributes), s[0].Attributes)
		}

		return nil
	}
}

func testPulsarSinkDestroy(s *terraform.State) error {
	client := getClientV3FromMeta(testAccProvider.Meta()).Sinks()
	for _, rs := range s.RootModule().Resources {
		if rs.Type != "pulsar_sink" {
			continue
		}
		sinkExists, err := testPulsarSinkExistsFromInstanceState(client, rs.Primary)
		if err != nil {
			return err
		}
		if sinkExists {
			return fmt.Errorf("ERROR_RESOURCE_SINK_STILL_EXISTS: %s", rs.Primary.ID)
		}
	}

	return nil
}

func testPulsarSinkExistsFromInstanceState(client pulsar.Sinks, instanceState *terraform.InstanceState) (bool, error) {
	tenant := instanceState.Attributes[resourceTenantAttribute]
	namespace := instanceState.Attributes[resourceNamespaceAttribute]

	sinks, err := client.ListSinks(tenant, namespace)
	if err != nil {
		return false, err
	}
	for _, sink := range sinks {
		if sink == instanceState.ID {
			return true, nil
		}
	}
	return false, nil
}

func testPulsarSink(wsURL, cluster, tenant, namespace, topic, sink string) string {
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
  tenant      = pulsar_tenant.test_tenant.tenant
  namespace   = "%s"
}

resource "pulsar_topic" "test_topic" {
  tenant     = pulsar_tenant.test_tenant.tenant
  namespace  = pulsar_namespace.test_namespace.namespace
  topic_name = "%s"
  topic_type = "persistent"
  partitions = 2
}

resource "pulsar_sink" "test" {
  tenant    = pulsar_tenant.test_tenant.tenant
  namespace = pulsar_namespace.test_namespace.namespace
  sink      = "%s"
  archive   = "%s"
  inputs    = [
    format(
      "%%s://%%s/%%s/%%s",
	  pulsar_topic.test_topic.topic_type,
	  pulsar_topic.test_topic.tenant,
	  pulsar_topic.test_topic.namespace,
	  pulsar_topic.test_topic.topic_name
	)
  ]
}
`, wsURL, cluster, tenant, namespace, topic, sink, githubArchiveFile)
}

// func testPulsarSinkWithUndefinedOptionalsInNsConf(wsURL, cluster, tenant, ns string) string {
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

// resource "pulsar_sink" "test" {
//   tenant    = pulsar_tenant.test_tenant.tenant
//   sink = "%s"

//   sink_config {
//     anti_affinity                  = "anti-aff"
//     max_producers_per_topic        = "50"
//   }

// }
// `, wsURL, cluster, tenant, ns)
// }

func testPulsarExistingSinkWithoutOptionals(wsURL, topic, sink string) string {
	return fmt.Sprintf(`
provider "pulsar" {
  web_service_url = "%s"
}

resource "pulsar_sink" "test" {
  tenant    = "public"
  namespace = "default"
  topic = "%s"
  sink = "%s"
}
`, wsURL, topic, sink)
}

// func testPulsarSinkWithPermissionGrants(wsURL, cluster, tenant, ns string, permissionGrants string) string {
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

// resource "pulsar_sink" "test" {
//   tenant    = pulsar_tenant.test_tenant.tenant
// 	sink = "%s"

// 	%s
// }
// `, wsURL, cluster, tenant, ns, permissionGrants)
// }
