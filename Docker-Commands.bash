# Create Containers
docker run -d   --name some-redis   -p 6379:6379   redis redis-server --save 60 1 --loglevel warning
docker run -d   -p 3000:3000   --name=grafana   -v grafana_data:/var/lib/grafana   grafana/grafana-enterprise
# Run after created
docker start some-redis
docker start grafana
