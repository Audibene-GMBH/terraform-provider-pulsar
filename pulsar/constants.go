package pulsar

const (
	resourceTenantAttribute    = "tenant"
	resourceNamespaceAttribute = "namespace"
	resourceNameAttribute      = "name"

	resourceSinkAttribute                 = "sink"
	resourceInputsAttribute               = "inputs"
	resourceArchiveAttribute              = "archive"
	resourceParallelismAttribute          = "parallelism"
	resourceProcessingGuaranteesAttribute = "processing_guarantees"
	resourceConfigsAttribute              = "configs"
	resourceCustomRuntimeOptionsAttribute = "custom_runtime_options"
	resourceRetainOrderingAttribute       = "retain_ordering"

	resourceSourceSubscriptionPositionAttribute = "subscription_position"
	subscriptionPositionEarliest                = "Earliest"
	subscriptionPositionLatest                  = "Latest"

	processingGuaranteeEffectivelyOnce = "EFFECTIVELY_ONCE"
	processingGuaranteeAtLeastOnce     = "ATLEAST_ONCE"
	processingGuaranteeAtMostOnce      = "ATMOST_ONCE"
)

var (
	supportedArchiveUrlSchemes = []string{"http://", "https://", "file://"}
	processingGuarantees       = []string{
		processingGuaranteeEffectivelyOnce,
		processingGuaranteeAtLeastOnce,
		processingGuaranteeAtMostOnce,
	}
	subscriptionPositions = []string{
		subscriptionPositionEarliest,
		subscriptionPositionLatest,
	}
)
