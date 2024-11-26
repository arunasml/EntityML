# Kafka setup on wsl2

## Start vpn kit and let the terminal running

```
wsl.exe -d wsl-vpnkit --cd /app wsl-vpnkit
```

## Dependencies
* Please ensure docker and docker compose are installed.


### Start docker service

* service docker start

### Set up logging
* cd to synthetic-data-generator/compose

```
 source cleanup.sh
 ```

### Run compose

```
docker compose up -d 
```

### Stop compose
```
docker compose down
```
