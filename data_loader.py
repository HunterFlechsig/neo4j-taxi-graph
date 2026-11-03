import pyarrow.parquet as pq
import pandas as pd
from neo4j import GraphDatabase, Query
from neo4j.exceptions import ServiceUnavailable, SessionExpired, TransientError
import sys
import time


class DataLoader:

    def __init__(self, uri, user, password):
        """
        Connect to the Neo4j database and other init steps
        
        Args:
            uri (str): URI of the Neo4j database
            user (str): Username of the Neo4j database
            password (str): Password of the Neo4j database
        """
        self.driver = GraphDatabase.driver(
            uri, auth=(user, password), encrypted=False,
            connection_timeout=5, connection_acquisition_timeout=5,
            max_transaction_retry_time=0,
        )
        try:
            self.driver.verify_connectivity()
            with self.driver.session() as session:
                session.run(Query("RETURN 1", timeout=5)).consume()
        except Exception:
            self.driver.close()
            raise


    def close(self):
        """
        Close the connection to the Neo4j database
        """
        self.driver.close()


    # Define a function to create nodes and relationships in the graph
    def load_transform_file(self, file_path):
        """
        Load the parquet file and transform it into a csv file
        Then load the csv file into neo4j

        Args:
            file_path (str): Path to the parquet file to be loaded
        """

        # Read the parquet file
        trips = pq.read_table(file_path)
        trips = trips.to_pandas()

        # Some data cleaning and filtering
        trips = trips[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'trip_distance', 'fare_amount']]

        # Filter out trips that are not in bronx
        bronx = [3, 18, 20, 31, 32, 46, 47, 51, 58, 59, 60, 69, 78, 81, 94, 119, 126, 136, 147, 159, 167, 168, 169, 174, 182, 183, 184, 185, 199, 200, 208, 212, 213, 220, 235, 240, 241, 242, 247, 248, 250, 254, 259]
        trips = trips[trips.iloc[:, 2].isin(bronx) & trips.iloc[:, 3].isin(bronx)]
        trips = trips[trips['trip_distance'] > 0.1]
        trips = trips[trips['fare_amount'] > 2.5]

        # Convert date-time columns to supported format
        trips['tpep_pickup_datetime'] = pd.to_datetime(trips['tpep_pickup_datetime'], format='%Y-%m-%d %H:%M:%S')
        trips['tpep_dropoff_datetime'] = pd.to_datetime(trips['tpep_dropoff_datetime'], format='%Y-%m-%d %H:%M:%S')
        
        # Convert to csv and store in the current directory
        save_loc = "/var/lib/neo4j/import/" + file_path.split(".")[0] + '.csv'
        trips.to_csv(save_loc, index=False)

        # TODO: Your code here
        trips['tpep_pickup_datetime'] = (
            trips['tpep_pickup_datetime']
            .dt.strftime('%Y-%m-%dT%H:%M:%S')
        )

        trips['tpep_dropoff_datetime'] = (
            trips['tpep_dropoff_datetime']
            .dt.strftime('%Y-%m-%dT%H:%M:%S')
        )

        trips_dict = trips.to_dict("records")

        query = """
        UNWIND $rows AS row

        MERGE (pickup:Location {name: row.PULocationID})
        MERGE (dropoff:Location {name: row.DOLocationID})

        CREATE (pickup)-[:TRIP {
            distance: row.trip_distance,
            fare: row.fare_amount,
            pickup_dt: datetime(row.tpep_pickup_datetime),
            dropoff_dt: datetime(row.tpep_dropoff_datetime)
        }]->(dropoff)
        """

        self.driver.execute_query(
            query,
            rows = trips_dict,
            database_="neo4j"
        )


def main():
    total_attempts = 10

    # Retry database startup only. Repeating a partially completed load can
    # create duplicate TRIP relationships.
    for attempt in range(1, total_attempts + 1):
        try:
            data_loader = DataLoader("neo4j://localhost:7687", "neo4j", "graphprocessing")
            break
        except (ServiceUnavailable, SessionExpired, TransientError) as e:
            print(f"(Attempt {attempt}/{total_attempts}) Database not ready: {e}", file=sys.stderr)
            if attempt < total_attempts:
                time.sleep(10)
        except Exception as e:
            print(f"Database connection failed: {e}", file=sys.stderr)
            return 1
    else:
        print("Database startup retries exhausted; data was not loaded.", file=sys.stderr)
        return 1

    try:
        data_loader.load_transform_file("yellow_tripdata_2022-03.parquet")
    except Exception as e:
        print(f"Data loading failed: {e}", file=sys.stderr)
        return 1
    finally:
        data_loader.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
