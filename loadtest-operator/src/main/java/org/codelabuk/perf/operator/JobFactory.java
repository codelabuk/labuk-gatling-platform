package org.codelabuk.perf.operator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvFromSource;
import io.fabric8.kubernetes.api.model.EnvFromSourceBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;

/**
 * Builds the four Jobs of a run. Every pod runs the perf image with the same tests (/tests), the same results
 * volume (/results) and the same environment, so a plan behaves identically in each phase.
 */
public final class JobFactory {

    public enum Role { validate, setup, workers, report }

    public static final String LABEL_LOADTEST = "perf.codelabuk.org/loadtest";
    public static final String LABEL_ROLE = "perf.codelabuk.org/role";
    static final String TESTS = "/tests";
    static final String RESULTS = "/results";

    private JobFactory() {}

    /** Job name for a role - kept short because Kubernetes copies it into a 63-character pod label. */
    public static String jobName(LoadTest lt, Role role) {
        String base = lt.getMetadata().getName();
        if (base.length() > 45) base = base.substring(0, 45);
        return base + "-" + role.name();
    }

    /** Run folder on the results volume or under the S3 prefix: unique per LoadTest, readable by people. */
    public static String runDir(LoadTest lt) {
        String uid = lt.getMetadata().getUid() == null ? "local" : lt.getMetadata().getUid().replace("-", "");
        String folder = lt.getMetadata().getName() + "-" + uid.substring(0, Math.min(8, uid.length()));
        LoadTestSpec.Results r = lt.getSpec().results;
        return (usesS3(lt.getSpec()) ? r.s3.url.replaceAll("/+$", "") : RESULTS) + "/" + folder;
    }

    static boolean usesS3(LoadTestSpec spec) {
        return spec.results != null && spec.results.s3 != null;
    }

    public static String planPath(LoadTestSpec spec) {
        return spec.plan.inImage || spec.plan.file.startsWith("/") ? spec.plan.file : TESTS + "/" + spec.plan.file;
    }

    /** Problems in the spec itself, before anything is created; null when fine. */
    public static String specProblem(LoadTestSpec spec) {
        if (spec == null) return "spec is missing";
        if (spec.image == null || spec.image.isBlank()) return "spec.image is required";
        if (spec.plan == null || spec.plan.file == null || spec.plan.file.isBlank()) return "spec.plan.file is required";
        int sources = (spec.plan.git != null ? 1 : 0) + (spec.plan.configMap != null ? 1 : 0) + (spec.plan.inImage ? 1 : 0)
                + (spec.plan.s3 != null ? 1 : 0);
        if (sources != 1) return "spec.plan needs exactly one of: git, configMap, inImage, s3";
        if (spec.plan.s3 != null && !spec.plan.s3.startsWith("s3://")) return "spec.plan.s3 must look like s3://bucket/folder";
        if (spec.plan.s3 != null && !usesS3(spec)) return "spec.plan.s3 needs results.s3 (its endpoint and credentials are used)";
        if (spec.plan.inImage && !spec.plan.file.startsWith("/")) return "spec.plan.file must be an absolute path when inImage is true";
        if (spec.workers < 1) return "spec.workers must be at least 1";
        if (spec.spread != null && !List.of("soft", "hard", "none").contains(spec.spread))
            return "spec.spread must be soft, hard or none - got '" + spec.spread + "'";
        boolean pvc = spec.results != null && spec.results.pvc != null && !spec.results.pvc.isBlank();
        if (pvc == usesS3(spec)) return "spec.results needs exactly one of: pvc, s3";
        if (usesS3(spec)) {
            if (spec.results.s3.url == null || !spec.results.s3.url.startsWith("s3://")) return "spec.results.s3.url must look like s3://bucket/prefix";
            if (spec.results.s3.credentialsSecret == null || spec.results.s3.credentialsSecret.isBlank())
                return "spec.results.s3.credentialsSecret is required (a Secret with AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)";
        }
        return null;
    }

    public static Job validate(LoadTest lt) {
        List<String> args = new ArrayList<>(List.of("validate", planPath(lt.getSpec()),
                "--workers", String.valueOf(lt.getSpec().workers), "--facts-file", "/dev/termination-log"));
        scenarioArgs(lt.getSpec(), args);
        return job(lt, Role.validate, args, Map.of(), 1);
    }

    public static Job setup(LoadTest lt, String runDir) {
        return job(lt, Role.setup, List.of("setup", planPath(lt.getSpec()), "--out", runDir + "/shared.json"), Map.of(), 1);
    }

    /** An Indexed Job: Kubernetes gives each pod JOB_COMPLETION_INDEX, which perf uses as its worker number. */
    public static Job workers(LoadTest lt, String runDir, String startAt, boolean sharedIn) {
        LoadTestSpec spec = lt.getSpec();
        Map<String, String> env = new LinkedHashMap<>();
        env.put("PLAN", planPath(spec));
        env.put("WORKER_COUNT", String.valueOf(spec.workers));
        env.put("START_AT", startAt);
        env.put("RESULTS_DIR", runDir + "/logs");
        if (sharedIn) env.put("SHARED_IN", runDir + "/shared.json");
        if (!spec.scenarios.isEmpty()) env.put("SCENARIO", String.join(",", spec.scenarios));
        boolean metrics = spec.metrics != null && spec.metrics.enabled;
        if (metrics) {
            env.put("METRICS_PORT", String.valueOf(spec.metrics.port));
            env.put("METRICS_LINGER_SECONDS", String.valueOf(spec.metrics.lingerSeconds));
        }
        Job job = job(lt, Role.workers, List.of("worker"), env, spec.workers);
        job.getSpec().setCompletionMode("Indexed");
        job.getSpec().setActiveDeadlineSeconds((long) spec.timeoutMinutes * 60 + spec.startDelaySeconds
                + (metrics ? spec.metrics.lingerSeconds : 0));
        Container perf = job.getSpec().getTemplate().getSpec().getContainers().get(0);
        perf.setResources(spec.resources);
        spreadWorkers(lt, job.getSpec().getTemplate().getSpec());
        if (metrics) {
            perf.setPorts(List.of(new io.fabric8.kubernetes.api.model.ContainerPortBuilder()
                    .withName("metrics").withContainerPort(spec.metrics.port).build()));
            // the de-facto standard annotations; a PodMonitor can select the role label instead
            job.getSpec().getTemplate().getMetadata().setAnnotations(Map.of(
                    "prometheus.io/scrape", "true",
                    "prometheus.io/port", String.valueOf(spec.metrics.port),
                    "prometheus.io/path", "/metrics"));
        }
        return job;
    }

    public static Job report(LoadTest lt, String runDir) {
        Job job = job(lt, Role.report, List.of("report", "--input", runDir + "/logs", "--output", runDir + "/report",
                "--name", "merged", "--facts-file", "/dev/termination-log"), Map.of(), 1);
        job.getSpec().getTemplate().getSpec().getContainers().get(0).setResources(lt.getSpec().reportResources);
        return job;
    }

    /**
     * Places the workers of a run on different nodes, so one node's CPU / network / ports do not limit the whole test.
     * soft: a topology spread constraint (max skew 1 per node, ScheduleAnyway) - best effort, never blocks a run;
     * hard: required pod anti-affinity - at most one worker per node; none: nothing added.
     */
    static void spreadWorkers(LoadTest lt, io.fabric8.kubernetes.api.model.PodSpec pod) {
        String mode = lt.getSpec().spread == null ? "soft" : lt.getSpec().spread;
        io.fabric8.kubernetes.api.model.LabelSelector sameRun = new io.fabric8.kubernetes.api.model.LabelSelectorBuilder()
                .addToMatchLabels(LABEL_LOADTEST, lt.getMetadata().getName())
                .addToMatchLabels(LABEL_ROLE, Role.workers.name())
                .build();
        String node = "kubernetes.io/hostname";
        switch (mode) {
            case "soft" -> pod.setTopologySpreadConstraints(List.of(new io.fabric8.kubernetes.api.model.TopologySpreadConstraintBuilder()
                    .withMaxSkew(1).withTopologyKey(node).withWhenUnsatisfiable("ScheduleAnyway").withLabelSelector(sameRun).build()));
            case "hard" -> pod.setAffinity(new io.fabric8.kubernetes.api.model.AffinityBuilder().withNewPodAntiAffinity()
                    .addNewRequiredDuringSchedulingIgnoredDuringExecution().withTopologyKey(node).withLabelSelector(sameRun).endRequiredDuringSchedulingIgnoredDuringExecution()
                    .endPodAntiAffinity().build());
            default -> { }
        }
    }

    private static void scenarioArgs(LoadTestSpec spec, List<String> args) {
        if (!spec.scenarios.isEmpty()) {
            args.add("--scenario");
            args.add(String.join(",", spec.scenarios));
        }
    }

    private static Job job(LoadTest lt, Role role, List<String> args, Map<String, String> extraEnv, int pods) {
        LoadTestSpec spec = lt.getSpec();
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("app.kubernetes.io/managed-by", "loadtest-operator");
        labels.put(LABEL_LOADTEST, lt.getMetadata().getName());
        labels.put(LABEL_ROLE, role.name());

        List<EnvVar> env = new ArrayList<>();
        spec.env.forEach((k, v) -> env.add(new EnvVarBuilder().withName(k).withValue(v).build()));
        extraEnv.forEach((k, v) -> env.add(new EnvVarBuilder().withName(k).withValue(v).build()));
        List<EnvFromSource> envFrom = new ArrayList<>();
        spec.secretRefs.forEach(s -> envFrom.add(new EnvFromSourceBuilder().withNewSecretRef().withName(s).endSecretRef().build()));
        if (usesS3(spec)) {
            LoadTestSpec.S3Results s3 = spec.results.s3;
            envFrom.add(new EnvFromSourceBuilder().withNewSecretRef().withName(s3.credentialsSecret).endSecretRef().build());
            env.add(new EnvVarBuilder().withName("AWS_REGION").withValue(s3.region).build());
            if (s3.endpoint != null && !s3.endpoint.isBlank()) env.add(new EnvVarBuilder().withName("S3_ENDPOINT").withValue(s3.endpoint).build());
        }

        List<Volume> volumes = new ArrayList<>();
        List<VolumeMount> mounts = new ArrayList<>();
        if (!usesS3(spec)) { // with S3 every pod reads and writes the bucket directly - no shared volume
            volumes.add(new VolumeBuilder().withName("results").withNewPersistentVolumeClaim().withClaimName(spec.results.pvc).endPersistentVolumeClaim().build());
            mounts.add(new VolumeMountBuilder().withName("results").withMountPath(RESULTS).build());
        }
        List<Container> init = new ArrayList<>();
        if (spec.plan.git != null) {
            volumes.add(new VolumeBuilder().withName("tests").withNewEmptyDir().endEmptyDir().build());
            init.add(gitClone(spec.plan.git));
        } else if (spec.plan.s3 != null) {
            volumes.add(new VolumeBuilder().withName("tests").withNewEmptyDir().endEmptyDir().build());
            init.add(s3Fetch(lt, role, env, envFrom));
        } else if (spec.plan.configMap != null) {
            volumes.add(new VolumeBuilder().withName("tests").withNewConfigMap().withName(spec.plan.configMap).endConfigMap().build());
        }
        if (!spec.plan.inImage) mounts.add(new VolumeMountBuilder().withName("tests").withMountPath(TESTS).withReadOnly(spec.plan.configMap != null).build());

        Container perf = new ContainerBuilder()
                .withName("perf")
                .withImage(spec.image)
                .withArgs(args)
                .withEnv(env)
                .withEnvFrom(envFrom)
                .withVolumeMounts(mounts)
                .withTerminationMessagePolicy("FallbackToLogsOnError")
                .build();

        return new JobBuilder()
                .withNewMetadata()
                    .withName(jobName(lt, role))
                    .withNamespace(lt.getMetadata().getNamespace())
                    .withLabels(labels)
                    .withOwnerReferences(new OwnerReferenceBuilder()
                            .withApiVersion(lt.getApiVersion()).withKind(lt.getKind())
                            .withName(lt.getMetadata().getName()).withUid(lt.getMetadata().getUid())
                            .withController(true).withBlockOwnerDeletion(true).build())
                .endMetadata()
                .withNewSpec()
                    .withCompletions(pods)
                    .withParallelism(pods)
                    .withBackoffLimit(0) // a retried worker would start late and skew the test - fail instead
                    .withTtlSecondsAfterFinished(spec.ttlSecondsAfterFinished)
                    .withNewTemplate()
                        .withNewMetadata().withLabels(labels).endMetadata()
                        .withSpec(new PodSpecBuilder()
                                .withRestartPolicy("Never")
                                .withInitContainers(init)
                                .withContainers(perf)
                                .withVolumes(volumes)
                                .withNodeSelector(spec.nodeSelector)
                                .withTolerations(spec.tolerations)
                                .withNewSecurityContext().withRunAsNonRoot(true).withRunAsUser(1000L).withFsGroup(1000L).endSecurityContext()
                                .build())
                    .endTemplate()
                .endSpec()
                .build();
    }

    /** Where the pods of a run read their tests when they come from S3: a copy in the run folder, made by validate. */
    public static String testsSnapshot(LoadTest lt) {
        return runDir(lt) + "/tests";
    }

    /**
     * Downloads the test folder from S3 with the runner image itself (perf fetch) - same credentials as the results.
     * The validate pod fetches spec.plan.s3 and stores a copy in the run folder; every later pod reads that copy, so a
     * plan edited during the run cannot change it halfway and the run folder shows exactly what ran.
     */
    private static Container s3Fetch(LoadTest lt, Role role, List<EnvVar> env, List<EnvFromSource> envFrom) {
        LoadTestSpec spec = lt.getSpec();
        List<String> args = role == Role.validate
                ? List.of("fetch", spec.plan.s3, TESTS, "--snapshot", testsSnapshot(lt))
                : List.of("fetch", testsSnapshot(lt), TESTS);
        return new ContainerBuilder()
                .withName("fetch-tests")
                .withImage(spec.image)
                .withArgs(args)
                .withEnv(env.stream().filter(e -> e.getName().startsWith("AWS_") || e.getName().equals("S3_ENDPOINT")).toList())
                .withEnvFrom(envFrom.stream().filter(e -> e.getSecretRef().getName().equals(spec.results.s3.credentialsSecret)).toList())
                .withVolumeMounts(new VolumeMountBuilder().withName("tests").withMountPath(TESTS).build())
                .build();
    }

    private static Container gitClone(LoadTestSpec.Git git) {
        List<EnvVar> env = new ArrayList<>(List.of(
                new EnvVarBuilder().withName("GIT_REPO").withValue(git.repo).build(),
                new EnvVarBuilder().withName("GIT_REF").withValue(git.ref).build(),
                new EnvVarBuilder().withName("HOME").withValue("/tmp").build()));
        if (git.tokenSecret != null)
            env.add(new EnvVarBuilder().withName("GIT_TOKEN").withNewValueFrom().withNewSecretKeyRef()
                    .withName(git.tokenSecret.name).withKey(git.tokenSecret.key).endSecretKeyRef().endValueFrom().build());
        // the token is passed via env, never written into the spec or logs
        // fetch by ref so GIT_REF can be a branch, a tag or a commit SHA (pinning a run to an exact commit)
        String script = "set -e; URL=\"$GIT_REPO\"; "
                + "if [ -n \"$GIT_TOKEN\" ]; then URL=$(echo \"$GIT_REPO\" | sed \"s#https://#https://oauth2:${GIT_TOKEN}@#\"); fi; "
                + "cd " + TESTS + "; git init --quiet; git remote add origin \"$URL\"; "
                + "git fetch --quiet --depth 1 origin \"$GIT_REF\"; git checkout --quiet FETCH_HEAD; "
                + "echo \"tests at $GIT_REPO@$GIT_REF ($(git rev-parse --short HEAD))\"";
        return new ContainerBuilder()
                .withName("clone-tests")
                .withImage(git.image)
                .withCommand("sh", "-c", script)
                .withEnv(env)
                .withVolumeMounts(new VolumeMountBuilder().withName("tests").withMountPath(TESTS).build())
                .build();
    }
}
