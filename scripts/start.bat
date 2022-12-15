docker stop et0_calculation
docker rm et0_calculation
docker build -t et0_calculation .
docker run --name et0_calculation --volume %cd%:/home --detach -t et0_calculation
docker exec et0_calculation python src/calculate_et0.py