
[![tests](https://github.com/ghga-de/internal-file-registry-service/actions/workflows/unit_and_int_tests.yaml/badge.svg)](https://github.com/ghga-de/internal-file-registry-service/actions/workflows/unit_and_int_tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/ghga-de/internal-file-registry-service/badge.svg?branch=main)](https://coveralls.io/github/ghga-de/internal-file-registry-service?branch=main)

# Internal File Registry Service

Internal-File-Registry-Service - This service acts as a registry for the internal location and representation of files.

## Description

<!-- Please provide a short overview of the features of this service.-->

This service provides functionality to administer files stored in an S3-compatible
object storage.
All file-related metadata is stored in an internal mongodb database, owned and controlled
by this service.
It exposes no REST API enpoints and communicates with other services via events.

### Events consumed:

#### files_to_register
This event signals that there is a file to register in the database.
The file-related metadata from this event gets saved in the database and the file is
moved from the incoming staging bucket to the permanent storage.

#### files_to_stage
This event signals that there is a file that needs to be staged for download.
The file is then copied from the permanent storage to the outbox for the actual download.
### Events published:

#### file_internally_registered
This event is published after a file was registered in the database.
It contains all the file-related metadata that was provided by the files_to_register event.

#### file_staged_for_download
This event is published after a file was successfully staged to the outbox.


## Installation
We recommend using the provided Docker container.

A pre-build version is available at [docker hub](https://hub.docker.com/repository/docker/ghga/internal-file-registry-service):
```bash
docker pull ghga/internal-file-registry-service:0.4.1
```

Or you can build the container yourself from the [`./Dockerfile`](./Dockerfile):
```bash
# Execute in the repo's root dir:
docker build -t ghga/internal-file-registry-service:0.4.1 .
```

For production-ready deployment, we recommend using Kubernetes, however,
for simple use cases, you could execute the service using docker
on a single server:
```bash
# The entrypoint is preconfigured:
docker run -p 8080:8080 ghga/internal-file-registry-service:0.4.1 --help
```

If you prefer not to use containers, you may install the service from source:
```bash
# Execute in the repo's root dir:
pip install .

# To run the service:
ifrs --help
```

## Configuration
### Parameters

The service requires the following configuration parameters:
- **`permanent_bucket`** *(string)*: The ID of the object storage bucket that is serving as permanent storage.

- **`file_registered_event_topic`** *(string)*: Name of the topic used for events indicating that a new file has been internally registered.

- **`file_registered_event_type`** *(string)*: The type used for events indicating that a new file has been internally registered.

- **`file_staged_event_topic`** *(string)*: Name of the topic used for events indicating that a new file has been internally registered.

- **`file_staged_event_type`** *(string)*: The type used for events indicating that a new file has been internally registered.

- **`file_deleted_event_topic`** *(string)*: Name of the topic used for events indicating that a file has been deleted.

- **`file_deleted_event_type`** *(string)*: The type used for events indicating that a file has been deleted.

- **`files_to_register_topic`** *(string)*: The name of the topic to receive events informing about new files to register.

- **`files_to_register_type`** *(string)*: The type used for events informing about new files to register.

- **`files_to_stage_topic`** *(string)*: The name of the topic to receive events informing about files to stage.

- **`files_to_stage_type`** *(string)*: The type used for events informing about a file to be staged.

- **`files_to_delete_topic`** *(string)*: The name of the topic to receive events informing about files to delete.

- **`files_to_delete_type`** *(string)*: The type used for events informing about a file to be deleted.

- **`service_name`** *(string)*: Default: `internal_file_registry`.

- **`service_instance_id`** *(string)*: A string that uniquely identifies this instance across all instances of this service. A globally unique Kafka client ID will be created by concatenating the service_name and the service_instance_id.

- **`kafka_servers`** *(array)*: A list of connection strings to connect to Kafka bootstrap servers.

  - **Items** *(string)*

- **`db_connection_str`** *(string)*: MongoDB connection string. Might include credentials. For more information see: https://naiveskill.com/mongodb-connection-string/.

- **`db_name`** *(string)*: Name of the database located on the MongoDB server.

- **`s3_endpoint_url`** *(string)*: URL to the S3 API.

- **`s3_access_key_id`** *(string)*: Part of credentials for login into the S3 service. See: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html.

- **`s3_secret_access_key`** *(string)*: Part of credentials for login into the S3 service. See: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html.

- **`s3_session_token`** *(string)*: Part of credentials for login into the S3 service. See: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html.

- **`aws_config_ini`** *(string)*: Path to a config file for specifying more advanced S3 parameters. This should follow the format described here: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-a-configuration-file.


### Usage:

A template YAML for configurating the service can be found at
[`./example-config.yaml`](./example-config.yaml).
Please adapt it, rename it to `.ifrs.yaml`, and place it into one of the following locations:
- in the current working directory were you are execute the service (on unix: `./.ifrs.yaml`)
- in your home directory (on unix: `~/.ifrs.yaml`)

The config yaml will be automatically parsed by the service.

**Important: If you are using containers, the locations refer to paths within the container.**

All parameters mentioned in the [`./example-config.yaml`](./example-config.yaml)
could also be set using environment variables or file secrets.

For naming the environment variables, just prefix the parameter name with `ifrs_`,
e.g. for the `host` set an environment variable named `ifrs_host`
(you may use both upper or lower cases, however, it is standard to define all env
variables in upper cases).

To using file secrets please refer to the
[corresponding section](https://pydantic-docs.helpmanual.io/usage/settings/#secret-support)
of the pydantic documentation.



## Architecture and Design:
<!-- Please provide an overview of the architecture and design of the code base.
Mention anything that deviates from the standard triple hexagonal architecture and
the corresponding structure. -->

This is a Python-based service following the Triple Hexagonal Architecture pattern.
It uses protocol/provider pairs and dependency injection mechanisms provided by the
[hexkit](https://github.com/ghga-de/hexkit) library.


## Development
For setting up the development environment, we rely on the
[devcontainer feature](https://code.visualstudio.com/docs/remote/containers) of vscode
in combination with Docker Compose.

To use it, you have to have Docker Compose as well as vscode with its "Remote - Containers"
extension (`ms-vscode-remote.remote-containers`) installed.
Then open this repository in vscode and run the command
`Remote-Containers: Reopen in Container` from the vscode "Command Palette".

This will give you a full-fledged, pre-configured development environment including:
- infrastructural dependencies of the service (databases, etc.)
- all relevant vscode extensions pre-installed
- pre-configured linting and auto-formating
- a pre-configured debugger
- automatic license-header insertion

Moreover, inside the devcontainer, a convenience commands `dev_install` is available.
It installs the service with all development dependencies, installs pre-commit.

The installation is performed automatically when you build the devcontainer. However,
if you update dependencies in the [`./setup.cfg`](./setup.cfg) or the
[`./requirements-dev.txt`](./requirements-dev.txt), please run it again.

## License
This repository is free to use and modify according to the
[Apache 2.0 License](./LICENSE).

## Readme Generation
This readme is autogenerate, please see [`readme_generation.md`](./readme_generation.md)
for details.
