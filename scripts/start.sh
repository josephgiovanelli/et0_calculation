#!/bin/bash
docker stop et0_calculation
docker rm et0_calculation
docker run -u $(id -u):$(id -g) --name et0_calculation --volume $(pwd):/home --detach -t et0_calculation
docker exec et0_calculation python ./src/main.py