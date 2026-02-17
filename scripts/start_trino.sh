# Will attempt to connect to postgres, please also run scripts/start_postgres.sh

cd soda-trino/local_instance
./generate_keys.sh
docker compose up --remove-orphans
cd ../../
