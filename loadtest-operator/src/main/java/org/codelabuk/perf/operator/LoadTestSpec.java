package org.codelabuk.perf.operator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import io.fabric8.generator.annotation.Min;
import io.fabric8.generator.annotation.Required;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Toleration;

public class LoadTestSpec {

    @Required
    @JsonPropertyDescription("perf runner image, e.g. registry/labuk-perf-runner:1.0.0")
    public String image;

    @Required
    @JsonPropertyDescription("Where the test plan comes from and which plan file to run")
    public Plan plan;

    @Min(1)
    @JsonPropertyDescription("Number of worker pods; all load figures in the plan are totals split across them")
    public int workers = 1;

    @JsonPropertyDescription("Run only these scenarios of the plan (default: all)")
    public List<String> scenarios = new ArrayList<>();

    @JsonPropertyDescription("Environment variables for every pod - fill ${NAME} placeholders in the plan")
    public Map<String, String> env = new LinkedHashMap<>();

    @JsonPropertyDescription("Secrets whose keys become environment variables (passwords, tokens) - never put them in env")
    public List<String> secretRefs = new ArrayList<>();

    @JsonPropertyDescription("CPU/memory for each worker; the JVM heap is 75% of the memory limit")
    public ResourceRequirements resources;

    public Map<String, String> nodeSelector = new LinkedHashMap<>();

    public List<Toleration> tolerations = new ArrayList<>();

    @JsonPropertyDescription("How worker pods are placed: soft (default) = one per node where possible, never blocks; "
            + "hard = at most one worker per node (extra workers stay Pending when there are fewer nodes); none = Kubernetes decides")
    public String spread = "soft";

    @JsonPropertyDescription("CPU/memory for the report pod (merges the workers' logs, builds the report); its JVM heap is 75% of "
            + "the memory limit - large runs need more (roughly 1 GB per 10 million requests on top of 1 GB)")
    public ResourceRequirements reportResources;

    @Required
    @JsonPropertyDescription("Where results and the merged report are written")
    public Results results;

    @Min(5)
    @JsonPropertyDescription("Seconds between creating the workers and their common start time (image pulls must fit in it)")
    public int startDelaySeconds = 60;

    @Min(1)
    @JsonPropertyDescription("Hard limit for the worker phase, in minutes")
    public int timeoutMinutes = 180;

    @JsonPropertyDescription("Seconds to keep finished Jobs and pods (logs) before Kubernetes deletes them")
    public int ttlSecondsAfterFinished = 3600;

    @JsonPropertyDescription("Live Prometheus metrics from every worker while the test runs")
    public Metrics metrics = new Metrics();

    public static class Metrics {
        @JsonPropertyDescription("Serve /metrics on each worker and add prometheus.io/* scrape annotations to the worker pods")
        public boolean enabled = false;

        public int port = 9464;

        @JsonPropertyDescription("Seconds a worker keeps serving its final values after its load ends, so the last scrape sees them")
        public int lingerSeconds = 30;
    }

    public static class Plan {
        @Required
        @JsonPropertyDescription("Plan file: relative to the repository / ConfigMap root, or an absolute path inside the image")
        public String file;

        @JsonPropertyDescription("Clone the tests from Git (recommended)")
        public Git git;

        @JsonPropertyDescription("Mount a ConfigMap as the tests folder (small, flat test sets - 1 MB limit)")
        public String configMap;

        @JsonPropertyDescription("The tests are baked into the image; `file` is an absolute path")
        public boolean inImage;

        @JsonPropertyDescription("Download the tests from S3 / MinIO (s3://bucket/folder) with the endpoint and credentials of results.s3")
        public String s3;
    }

    public static class Git {
        @Required
        public String repo;

        public String ref = "main";

        @JsonPropertyDescription("Secret with the access token for an https repository")
        public SecretKey tokenSecret;

        @JsonPropertyDescription("Image used to clone")
        public String image = "alpine/git:2.47.2";
    }

    public static class SecretKey {
        @Required
        public String name;

        public String key = "token";
    }

    /** Exactly one of pvc / s3. */
    public static class Results {
        @JsonPropertyDescription("Existing PersistentVolumeClaim shared by all pods of the run (ReadWriteMany on multi-node clusters)")
        public String pvc;

        @JsonPropertyDescription("S3 or MinIO bucket instead of a volume - no shared storage needed")
        public S3Results s3;
    }

    public static class S3Results {
        @Required
        @JsonPropertyDescription("s3://bucket/prefix - each run gets a folder below it")
        public String url;

        @JsonPropertyDescription("e.g. http://minio.perf:9000 for MinIO; leave empty for AWS S3")
        public String endpoint;

        public String region = "us-east-1";

        @Required
        @JsonPropertyDescription("Secret with AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        public String credentialsSecret;
    }
}
