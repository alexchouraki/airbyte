#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_athena_health import SourceAthenaHealth

if __name__ == "__main__":
    source = SourceAthenaHealth()
    launch(source, sys.argv[1:])
