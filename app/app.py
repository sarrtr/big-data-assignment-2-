#!/usr/bin/env python3
from cassandra.cluster import Cluster


HOSTS = ["cassandra-server"]


def main():
    cluster = Cluster(HOSTS)
    session = cluster.connect()
    try:
        keyspaces = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        print("Connected to Cassandra. Available keyspaces:")
        for row in keyspaces:
            print(row.keyspace_name)
    finally:
        session.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    main()
