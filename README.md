![tests](https://github.com/ghga-de/internal-file-registry-service/actions/workflows/unit_and_int_tests.yaml/badge.svg)
[![codecov](https://codecov.io/gh/ghga-de/internal-file-registry-service/branch/main/graph/badge.svg?token=GYH99Y71CK)](https://codecov.io/gh/ghga-de/internal-file-registry-service)
# Internal-File-Registry-Service

This service acts as a registry for the internal location and representation of files. Upon providing an internal file ID, it delivers a URL to the corresponding file. If the file is located on a permanent storage system that is not accessible to other services (i.e. not S3, e.g. a flat-file system), this service SHOULD stage the file into an accessible S3-based storage system and serve the location to the staged file.

## Documentation:

An extensive documentation can be found [here](...) (coming soon).

## Quick Start
### Installation
We recommend using the provided Docker container.

A pre-build version is available at [docker hub](https://hub.docker.com/repository/docker/ghga/internal-file-registry-service):
```bash
docker pull ghga/internal-file-registry-service:<version>
```

Or you can build the container yourself from the [`./Dockerfile`](./Dockerfile):
```bash
# Execute in the repo's root dir:
# (Please feel free to adapt the name/tag.)
docker build -t ghga/internal-file-registry-service:<version> .
```

For production-ready deployment, we recommend using Kubernetes, however,
for simple use cases, you could execute the service using docker
on a single server:
```bash
# The entrypoint is preconfigured:
docker run -p 8080:8080 ghga/internal-file-registry-service:<version>
```

If you prefer not to use containers, you may install the service from source:
```bash
# Execute in the repo's root dir:
pip install .

# to run the service:
internal-file-registry-service
```

### Configuration:
The [`./example_config.yaml`](./example_config.yaml) gives an overview of the available configuration options.
Please adapt it, rename it to `.internal_file_registry_service.yaml`, and place it to one of the following locations:
- in the current working directory were you are execute the service (on unix: `./.internal_file_registry_service.yaml`)
- in your home directory (on unix: `~/.internal_file_registry_service.yaml`)

The config yaml will be automatically parsed by the service.

**Important: If you are using containers, the locations refer to paths within the container.**

All parameters mentioned in the [`./example_config.yaml`](./example_config.yaml)
could also be set using environment variables or file secrets.

For naming the environment variables, just prefix the parameter name with `MY_MICROSERVICE_`,
e.g. for the `host` set an environment variable named `MY_MICROSERVICE_HOST`
(you may use both upper or lower cases, however, it is standard to define all env
variables in upper cases).

To using file secrets please refer to the
[corresponding section](https://pydantic-docs.helpmanual.io/usage/settings/#secret-support)
of the pydantic documentation.


## Development
For setting up the development environment, we rely on the
[devcontainer feature](https://code.visualstudio.com/docs/remote/containers) of vscode
in combination with Docker Compose.

To use it, you have to have Docker Compose as well as vscode with its "Remote - Containers" extension (`ms-vscode-remote.remote-containers`) installed.
Then open this repository in vscode and run the command
`Remote-Containers: Reopen in Container` from the vscode "Command Palette".

This will give you a full-fledged, pre-configured development environment including:
- infrastructural dependencies of the service (databases, etc.)
- all relevant vscode extensions pre-installed
- pre-configured linting and auto-formating
- a pre-configured debugger
- automatic license-header insertion

Moreover, inside the devcontainer, there are two convenience commands available
(please type them in the integrated terminal of vscode):
- `dev_install` - install the service with all development dependencies,
installs pre-commit, and applies any migration scripts to the test database
(please run that if you are starting the devcontainer for the first time
or if you added any python dependencies to the [`./setup.cfg`](./setup.cfg))
- `dev_launcher` - starts the service with the development config yaml
(located in the `./.devcontainer/` dir)

If you prefer not to use vscode, you could get a similar setup (without the editor specific features)
by running the following commands:
``` bash
# Execute in the repo's root dir:
cd ./.devcontainer

# build and run the environment with docker-compose
docker-compose up

# attach to the main container:
# (you can open multiple shell sessions like this)
docker exec -it devcontainer_app_1 /bin/bash
```

## License
This repository is free to use and modify according to the [Apache 2.0 License](./LICENSE).
