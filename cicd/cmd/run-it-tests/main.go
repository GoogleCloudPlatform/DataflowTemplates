package main

import (
	"flag"
	"log"

	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/flags"
	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/workflows"
)

func main() {
	flags.RegisterCommonFlags()
	flags.RegisterItFlags()
	flag.Parse()

	// Run mvn install first to ensure all dependencies are available
	mvnFlags := workflows.NewMavenFlags()
	err := workflows.MvnCleanInstall().Run(
		mvnFlags.IncludeDependencies(),
		mvnFlags.IncludeDependents(),
		mvnFlags.SkipDependencyAnalysis(),
		mvnFlags.SkipCheckstyle(),
		mvnFlags.SkipJib(),
		mvnFlags.SkipTests(),
		mvnFlags.SkipJacoco(),
		mvnFlags.SkipShade(),
		mvnFlags.ThreadCount(8),
		mvnFlags.InternalMaven())
	if err != nil {
		log.Fatalf("%v\n", err)
	}

	// Run only Spanner-specific integration tests
	mvnFlags = workflows.NewMavenFlags()
	err = workflows.MvnVerify().Run(
		mvnFlags.IncludeDependencies(),
		mvnFlags.IncludeDependents(),
		mvnFlags.SkipDependencyAnalysis(),
		mvnFlags.SkipCheckstyle(),
		mvnFlags.SkipJib(),
		mvnFlags.SkipShade(),
		mvnFlags.RunIntegrationTests(flags.UnifiedWorkerHarnessContainerImage() != ""),
		mvnFlags.ThreadCount(4),
		mvnFlags.IntegrationTestParallelism(3),
		// Only include Spanner instance configuration
		mvnFlags.StaticSpannerInstance("spanner-demo"),
		mvnFlags.InternalMaven(),
		flags.Region(),
		flags.Project(),
		flags.ArtifactBucket(),
		flags.StageBucket(),
		flags.HostIp(),
		flags.PrivateConnectivity(),
		flags.SpannerHost(),
		flags.FailureMode(),
		flags.RetryFailures(),
		flags.UnifiedWorkerHarnessContainerImage())
	if err != nil {
		log.Fatalf("%v\n", err)
	}
	log.Println("Build and Spanner Integration Tests Successful!")
}