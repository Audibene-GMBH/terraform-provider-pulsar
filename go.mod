module github.com/streamnative/terraform-provider-pulsar

go 1.13

require (
	github.com/aws/aws-sdk-go v1.25.48 // indirect
	github.com/hashicorp/go-cty v1.4.1-0.20200414143053-d3edf31b6320
	github.com/hashicorp/go-multierror v1.0.0
	github.com/hashicorp/terraform-plugin-sdk/v2 v2.7.0
	github.com/olekukonko/tablewriter v0.0.4 // indirect
	github.com/pkg/errors v0.9.1
	// Current master commit
	github.com/streamnative/pulsarctl v0.4.3-0.20210916133840-55ffee240e46
)

replace github.com/streamnative/pulsarctl => github.com/Audibene-GMBH/pulsarctl v0.5.0-backport
