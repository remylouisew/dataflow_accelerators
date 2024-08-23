#fixed a lot of issues with dockerfile by building locally
docker build . -t remytest
docker image tag remytest us-central1-docker.pkg.dev/cool-machine-learning/axlearn/axlearn-dataflow:remywtest
docker image push  us-central1-docker.pkg.dev/cool-machine-learning/axlearn/axlearn-dataflow:remywtest


DOCKER_REPO=us-central1-docker.pkg.dev/cool-machine-learning/axlearn
DOCKER_IMAGE=axlearn-dataflow

axlearn gcp dataflow start \
--dataflow_spec=runner=DirectRunner \
    --bundler_spec=dockerfile=Dockerfile \
    --bundler_spec=repo=us-central1-docker.pkg.dev/cool-machine-learning/axlearn \
    --bundler_spec=image=axlearn-dataflow \
    -- "'rm -r /tmp/output_dir; \
    python3 -m apache_beam.examples.wordcount \
        --input=gs://dataflow-samples/shakespeare/kinglear.txt \
        --output=/tmp/output_dir/outputs
       '"

axlearn gcp dataflow start \
    --bundler_spec=dockerfile=Dockerfile \
    --bundler_spec=repo=${DOCKER_REPO} \
    --bundler_spec=image=${DOCKER_IMAGE} \
    --bundler_spec=allow_dirty=True \
    --dataflow_spec=runner=DataflowRunner \
    --dataflow_spec=dataflow_service_options="worker_accelerator=type:nvidia-l4;count:1;install-nvidia-driver" \
    -- "'python3 -m apache_beam.examples.wordcount \
        --input=gs://dataflow-samples/shakespeare/kinglear.txt \
        --output=/tmp/output_dir/outputs \
       '"

axlearn gcp dataflow start \
    --bundler_spec=dockerfile=Dockerfile \
    --bundler_spec=repo=${DOCKER_REPO} \
    --bundler_spec=image=${DOCKER_IMAGE} \
    --bundler_spec=allow_dirty=True \
    --dataflow_spec=runner=DataflowRunner \
    --dataflow_spec=dataflow_service_options="enable_secure_boot,enable_google_cloud_heap_sampling,worker_accelerator=type:nvidia-l4;count:1;install-nvidia-driver" \
    -- "'
    python3 -m apache_beam.examples.wordcount \
        --input=gs://dataflow-samples/shakespeare/kinglear.txt \
        --output=gsL///output_dir/outputs \
       '"

--dataflow_spec="dataflow_service_options=enable_secure_boot,enable_google_cloud_heap_sampling,some_other_option" \

--name=remyw-dataflow1 \
    #--bundler_spec=base_image=apache/beam_python3.10_sdk:2.52.0 \
    #--bundler_spec=target=dataflow \
