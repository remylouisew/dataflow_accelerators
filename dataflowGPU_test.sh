#fixed a lot of issues with dockerfile by building locally
docker build . -t remytest
docker image tag remytest us-central1-docker.pkg.dev/cool-machine-learning/axlearn/axlearn-dataflow:remywtest
docker image push  us-central1-docker.pkg.dev/cool-machine-learning/axlearn/axlearn-dataflow:remywtest


DOCKER_REPO=us-central1-docker.pkg.dev/cool-machine-learning/axlearn/axlearn-dataflow
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

# axlearn on CPU - works
axlearn gcp dataflow start \
    --name=$USER-dataflow \
    --bundler_spec=dockerfile=Dockerfile \
    --bundler_spec=target=dataflow \
    --bundler_spec=allow_dirty=True \
    --dataflow_spec=runner=DataflowRunner \
    --dataflow_spec=region=us-central1 \
    --dataflow_spec=machine_type=n2-standard-8 \
    -- python3 -m apache_beam.examples.wordcount \
            --input=gs://dataflow-samples/shakespeare/kinglear.txt \
        --output=gs://ttl-30d-us-central2/axlearn/users/jesusfc/dataflow/wordcount


# try with GPUs...doesn't work because can't parse dataflow_service_options
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

#try with new dockerfile 08/28
DOCKER_REPO=us-central1-docker.pkg.dev/cool-machine-learning/axlearn
DOCKER_IMAGE=maingpu

axlearn gcp dataflow start \
    --bundler_spec=dockerfile=Dockerfile \
    --bundler_spec=repo=${DOCKER_REPO} \
    --bundler_spec=image=${DOCKER_IMAGE} \
    --bundler_spec=allow_dirty=True \
    --dataflow_spec=runner=DataflowRunner \
    --dataflow_spec=dataflow_service_options="'worker_accelerator=type:nvidia-l4;count:1;install-nvidia-driver'" \
    -- "'
    python3 -m axlearn.cloud.gcp.examples.mainGPU 
       '"

axlearn gcp dataflow start \


--dataflow_spec="dataflow_service_options=enable_secure_boot,enable_google_cloud_heap_sampling,some_other_option" \

--name=remyw-dataflow1 \
    #--bundler_spec=base_image=apache/beam_python3.10_sdk:2.52.0 \
    #--bundler_spec=target=dataflow \

#try with flat formatting
#and hardocded dataflow_service_options
axlearn gcp dataflow start --bundler_spec=dockerfile=Dockerfile --bundler_spec=repo=us-central1-docker.pkg.dev/cool-machine-learning/axlearn --bundler_spec=image=axlearn-dataflow --bundler_spec=target=dataflow --bundler_spec=allow_dirty=True --dataflow_spec=runner=DataflowRunner -- "python3 -m apache_beam.examples.wordcount --input=gs://dataflow-samples/shakespeare/kinglear.txt --output=gs://dataflow-test-bucket0/rwtest/output/ "

# from Zhiyun Lu

DOCKER_REPO=us-docker.pkg.dev/cybertron-gcp-island-test-0rxn/applejax
DOCKER_IMAGE=ajax-dataflow
DOCKER_TAG=generate_preprocessed_example_${USER}2
PERM_BUCKET="ttl-30d-us-central2-0rxn"

DATA_DIR="gs://permanent-us-central2-0rxn/tensorflow_datasets"
OUTPUT_DATA_DIR="gs://ttl-30d-us-central2-0rxn/tensorflow_datasets/${USER}/"
CONFIG_MODULE=mixture_general_lm
CONFIG_NAME=v1-85m
OUTPUT_DATASET=mixture_general_lm_4t2
NUM_SHARDS=10
MAX_EXAMPLES_PER_SHARD=100
axlearn gcp dataflow start \
    --name=preprocess-${USER}2 \
    --dataflow_spec=runner=DataflowRunner \
    --dataflow_spec=disk_size_gb=64 \
    --dataflow_spec=resource_hints=min_ram=64GB \
    --dataflow_spec=number_of_worker_harness_threads=1 \
    --vm_type=n1-standard-32 \
    --bundler_spec=platform=linux/amd64 \
    --bundler_spec=dockerfile=ajax/tools/Dockerfile.dataflow \
    --bundler_spec=DATAFLOW_PYTHON_VERSION=3.9 \
    --bundler_spec=image=${DOCKER_IMAGE} \
    --bundler_spec=repo=${DOCKER_REPO} \
    -- "'
  python3 -m ajax.tools.generate_deterministic_input_examples \
    --config_module=${CONFIG_MODULE} --config=${CONFIG_NAME} \
    --input_data_dir=${DATA_DIR} \
    --output_data_dir=${OUTPUT_DATA_DIR} \
    --output_dataset=${OUTPUT_DATASET} \
    --num_shards=${NUM_SHARDS} \
    --max_examples_per_shard=${MAX_EXAMPLES_PER_SHARD}'"