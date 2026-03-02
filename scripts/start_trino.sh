# Will attempt to connect to postgres, please also run scripts/start_postgres.sh

cd soda-trino/local_instance
./generate_keys.sh
# for now this is called in CI hence we need -d
docker compose up --remove-orphans -d 
cd ../../
