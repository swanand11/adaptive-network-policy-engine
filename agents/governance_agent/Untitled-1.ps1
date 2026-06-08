docker logs service-agent 2>&1 | grep -i "partition assignment"
docker logs service-agent-2 2>&1 | grep -i "partition assignment"
docker logs service-agent-3 2>&1 | grep -i "partition assignment"