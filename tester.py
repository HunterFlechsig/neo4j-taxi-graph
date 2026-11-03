import time
import requests
import interface
from neo4j import GraphDatabase, Query
from neo4j.exceptions import AuthError, ServiceUnavailable, SessionExpired, TransientError
from collections import deque
from collections.abc import Mapping
import sys
import warnings
import logging
import os
from contextlib import contextmanager

# Suppress Neo4j driver warnings and notifications
warnings.filterwarnings("ignore")
logging.getLogger("neo4j").setLevel(logging.ERROR)

@contextmanager
def suppress_neo4j_notifications():
    """Context manager to suppress Neo4j server notifications"""
    # Save original stderr
    original_stderr = sys.stderr
    try:
        # Redirect stderr to devnull
        sys.stderr = open(os.devnull, 'w')
        yield
    finally:
        # Restore original stderr
        sys.stderr.close()
        sys.stderr = original_stderr

def wait_for_server(timeout_seconds=240, retry_interval=5):
    """Wait for HTTP, authenticated Bolt, and an executable database query."""
    deadline = time.monotonic() + timeout_seconds
    last_error = "readiness check timed out"
    while time.monotonic() < deadline:
        driver = None
        try:
            with requests.get("http://localhost:7474/", timeout=(3, 3)) as response:
                response.raise_for_status()
            driver = GraphDatabase.driver(
                "neo4j://localhost:7687", auth=("neo4j", "graphprocessing"),
                encrypted=False, connection_timeout=5,
                connection_acquisition_timeout=5, max_transaction_retry_time=0,
            )
            driver.verify_connectivity()
            with driver.session() as session:
                session.run(Query("RETURN 1", timeout=5)).consume()
            print(" ✓")
            print("HTTP, authenticated Bolt, and database query are ready!\n")
            return True
        except AuthError:
            print("\n✗ ERROR: Neo4j authentication failed. Expected neo4j / graphprocessing.")
            return False
        except (requests.RequestException, ServiceUnavailable, SessionExpired, TransientError) as e:
            last_error = str(e)
        except Exception as e:
            print(f"\n✗ ERROR: Readiness check failed: {e}")
            return False
        finally:
            if driver is not None:
                driver.close()
        remaining = deadline - time.monotonic()
        if remaining > 0:
            print(".", end="", flush=True)
            time.sleep(min(retry_interval, remaining))
    print(f"\n✗ ERROR: Neo4j was not ready within {timeout_seconds} seconds: {last_error}")
    print("Please check the container logs, ports, password, and database status.")
    return False


def close_interface(conn):
    """Release each student connection without masking its test result."""
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass


def read_bfs_graph():
    """Read the directed Location/TRIP graph without changing it."""
    with GraphDatabase.driver(
        "neo4j://localhost:7687", auth=("neo4j", "graphprocessing"),
        encrypted=False, connection_timeout=5, connection_acquisition_timeout=5,
    ) as driver:
        with driver.session() as session:
            nodes = session.run("MATCH (n:Location) RETURN n.name AS name").data()
            adjacency = {row["name"]: set() for row in nodes}
            edges = session.run(
                "MATCH (a:Location)-[:TRIP]->(b:Location) "
                "RETURN DISTINCT a.name AS source, b.name AS target"
            ).data()
            for row in edges:
                adjacency[row["source"]].add(row["target"])
    return adjacency


def validate_bfs_result(result, start_node, last_node, adjacency):
    """Validate a BFS traversal prefix; same-depth visit order may vary."""
    if not isinstance(result, list) or not result or not isinstance(result[0], Mapping):
        return False, "Expected a non-empty list of records with a path field"
    path = result[0].get("path")
    if hasattr(path, "nodes"):
        nodes = list(path.nodes)
    elif isinstance(path, (list, tuple)) and path:
        if all(isinstance(item, Mapping) for item in path):
            nodes = list(path)
        elif (len(path) % 2 == 1
              and all(isinstance(item, Mapping) for item in path[::2])
              and all(isinstance(item, str) and item for item in path[1::2])):
            # Result.data() exports a Neo4j Path as node, relationship type,
            # node, ... . GDS uses virtual NEXT edges for traversal order.
            nodes = list(path[::2])
        else:
            return False, "Invalid path representation"
    else:
        return False, "Expected a non-empty path"

    names = [node.get("name") for node in nodes]
    if not names or any(type(name) is not int for name in names):
        return False, "Every visited node must have an integer name"
    if names[0] != start_node or names[-1] != last_node:
        return False, f"Expected traversal from {start_node} to {last_node}"
    if len(set(names)) != len(names):
        return False, "A BFS traversal must not visit a node more than once"
    if any(name not in adjacency for name in names):
        return False, "Traversal contains a node outside the loaded Location graph"

    # Distances follow the original pickup -> dropoff direction. Consecutive
    # visited nodes need not have a TRIP edge between them.
    distance = {start_node: 0}
    queue = deque([start_node])
    while queue:
        node = queue.popleft()
        for neighbor in adjacency[node]:
            if neighbor not in distance:
                distance[neighbor] = distance[node] + 1
                queue.append(neighbor)
    if last_node not in distance:
        return False, "Target is not reachable from the source"
    if any(name not in distance for name in names):
        return False, "Traversal contains an unreachable node"
    depths = [distance[name] for name in names]
    if depths != sorted(depths):
        return False, "BFS must visit nodes in nondecreasing hop distance"
    required = {name for name, depth in distance.items() if depth < distance[last_node]}
    if not required.issubset(names):
        return False, "Traversal skipped nodes at a depth before the target"
    return True, "Valid BFS traversal prefix"


class TesterConnect:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        self._driver.verify_connectivity()

    def close(self):
        self._driver.close()

    def test_data_loaded(self):
        """
        Test to see if data is loaded into the database
            2. Run a query to get the number of nodes
            3. Run a query to get the number of edges
            4. Compare the results with the expected results
        """

        print("=" * 60)
        print("PART 1: DATA LOADING TESTS")
        print("=" * 60)

        points = 0
        total_points = 10

        with self._driver.session() as session:
            query = """
                MATCH (n)
                RETURN count(n) as num_nodes
            """
            result = session.run(query)
            num_nodes = result.data()[0]['num_nodes']

            query = """
                MATCH ()-[r]->()
                RETURN count(r) as num_edges
            """
            result = session.run(query)
            num_edges = result.data()[0]['num_edges']

            print(f"\nNode Count Test (5 points):")
            print(f"  Expected: 42 nodes")
            print(f"  Actual:   {num_nodes} nodes")
            if num_nodes == 42:
                print("  ✓ PASS [5/5 points]")
                points += 5
            else:
                print("  ✗ FAIL [0/5 points]")

            print(f"\nEdge Count Test (5 points):")
            print(f"  Expected: 1530 edges")
            print(f"  Actual:   {num_edges} edges")
            if num_edges == 1530:
                print("  ✓ PASS [5/5 points]")
                points += 5
            else:
                print("  ✗ FAIL [0/5 points]")

        print(f"\n{'─' * 60}")
        print(f"Part 1 Score: {points}/{total_points} points")
        print(f"{'─' * 60}\n")
        return points


def test_page_rank_comprehensive():
    """
    Comprehensive PageRank tests with multiple test cases
    to prevent hardcoded answers
    """

    print("=" * 60)
    print("PART 2: PAGERANK TESTS (GRADER VERSION)")
    print("=" * 60)

    points = 0
    total_points = 25
    test_cases_passed = 0
    total_test_cases = 5

    # Test Case 1: max_iterations=20, weight_property="distance"
    print(f"\n[Test 1/5] PageRank with max_iterations=20, weight='distance'")
    conn = None
    try:
        conn = interface.Interface("neo4j://localhost:7687", "neo4j", "graphprocessing")
        result = conn.pagerank(20, "distance")

        if not result or len(result) < 2:
            print(f"  ✗ FAIL: Invalid return format")
        else:
            max_node = result[0]
            min_node = result[1]

            max_correct = max_node.get('name') == 159 and round(max_node.get('score', 0), 5) == 3.22825
            min_correct = min_node.get('name') == 59 and round(min_node.get('score', 0), 5) == 0.18247

            if max_correct and min_correct:
                print(f"  ✓ PASS")
                test_cases_passed += 1
            else:
                print(f"  ✗ FAIL")
                print(f"    Expected: max=(159, 3.22825), min=(59, 0.18247)")
                print(f"    Actual: max=({max_node.get('name')}, {round(max_node.get('score', 0), 5)}), min=({min_node.get('name')}, {round(min_node.get('score', 0), 5)})")
    except Exception as e:
        print(f"  ✗ FAIL: {str(e)}")
    finally:
        close_interface(conn)

    # Test Case 2: max_iterations=10, weight_property="distance"
    print(f"\n[Test 2/5] PageRank with max_iterations=10, weight='distance'")
    conn = None
    try:
        conn = interface.Interface("neo4j://localhost:7687", "neo4j", "graphprocessing")
        result = conn.pagerank(10, "distance")

        if not result or len(result) < 2:
            print(f"  ✗ FAIL: Invalid return format")
        else:
            max_node = result[0]
            min_node = result[1]

            # Different iterations should give slightly different scores
            max_correct = max_node.get('name') == 159 and round(max_node.get('score', 0), 5) == 2.59893
            min_correct = min_node.get('name') == 59 and round(min_node.get('score', 0), 5) == 0.17706

            if max_correct and min_correct:
                print(f"  ✓ PASS")
                test_cases_passed += 1
            else:
                print(f"  ✗ FAIL")
                print(f"    Expected: max=(159, 2.59893), min=(59, 0.17706)")
                print(f"    Actual: max=({max_node.get('name')}, {round(max_node.get('score', 0), 5)}), min=({min_node.get('name')}, {round(min_node.get('score', 0), 5)})")
    except Exception as e:
        print(f"  ✗ FAIL: {str(e)}")
    finally:
        close_interface(conn)

    # Test Case 3: max_iterations=5, weight_property="distance"
    print(f"\n[Test 3/5] PageRank with max_iterations=5, weight='distance'")
    conn = None
    try:
        conn = interface.Interface("neo4j://localhost:7687", "neo4j", "graphprocessing")
        result = conn.pagerank(5, "distance")

        if not result or len(result) < 2:
            print(f"  ✗ FAIL: Invalid return format")
        else:
            max_node = result[0]
            min_node = result[1]

            max_correct = max_node.get('name') == 159 and round(max_node.get('score', 0), 5) == 1.59583
            min_correct = min_node.get('name') == 59 and round(min_node.get('score', 0), 5) == 0.16844

            if max_correct and min_correct:
                print(f"  ✓ PASS")
                test_cases_passed += 1
            else:
                print(f"  ✗ FAIL")
                print(f"    Expected: max=(159, 1.59583), min=(59, 0.16844)")
                print(f"    Actual: max=({max_node.get('name')}, {round(max_node.get('score', 0), 5)}), min=({min_node.get('name')}, {round(min_node.get('score', 0), 5)})")
    except Exception as e:
        print(f"  ✗ FAIL: {str(e)}")
    finally:
        close_interface(conn)

    # Test Case 4: max_iterations=15, weight_property="distance"
    print(f"\n[Test 4/5] PageRank with max_iterations=15, weight='distance'")
    conn = None
    try:
        conn = interface.Interface("neo4j://localhost:7687", "neo4j", "graphprocessing")
        result = conn.pagerank(15, "distance")

        if not result or len(result) < 2:
            print(f"  ✗ FAIL: Invalid return format")
        else:
            max_node = result[0]
            min_node = result[1]

            # Using 15 iterations gives different results
            max_correct = max_node.get('name') == 159 and round(max_node.get('score', 0), 5) == 3.03687
            min_correct = min_node.get('name') == 59 and round(min_node.get('score', 0), 5) == 0.18083

            if max_correct and min_correct:
                print(f"  ✓ PASS")
                test_cases_passed += 1
            else:
                print(f"  ✗ FAIL")
                print(f"    Expected: max=(159, 3.03687), min=(59, 0.18083)")
                print(f"    Actual: max=({max_node.get('name')}, {round(max_node.get('score', 0), 5)}), min=({min_node.get('name')}, {round(min_node.get('score', 0), 5)})")
    except Exception as e:
        print(f"  ✗ FAIL: {str(e)}")
    finally:
        close_interface(conn)

    # Test Case 5: max_iterations=20, weight_property="fare"
    print(f"\n[Test 5/5] PageRank with max_iterations=20, weight='fare'")
    conn = None
    try:
        conn = interface.Interface("neo4j://localhost:7687", "neo4j", "graphprocessing")
        result = conn.pagerank(20, "fare")

        if not result or len(result) < 2:
            print(f"  ✗ FAIL: Invalid return format")
        else:
            max_node = result[0]
            min_node = result[1]

            # Fare weighting detects implementations that ignore weight_property
            max_correct = max_node.get('name') == 242 and round(max_node.get('score', 0), 5) == 2.12298
            min_correct = min_node.get('name') == 59 and round(min_node.get('score', 0), 5) == 0.18723

            if max_correct and min_correct:
                print(f"  ✓ PASS")
                test_cases_passed += 1
            else:
                print(f"  ✗ FAIL")
                print(f"    Expected: max=(242, 2.12298), min=(59, 0.18723)")
                print(f"    Actual: max=({max_node.get('name')}, {round(max_node.get('score', 0), 5)}), min=({min_node.get('name')}, {round(min_node.get('score', 0), 5)})")
    except Exception as e:
        print(f"  ✗ FAIL: {str(e)}")
    finally:
        close_interface(conn)

    # Calculate points based on test cases passed
    if test_cases_passed == 5:
        points = 25
        print(f"\n  ✓ ALL TESTS PASSED [25/25 points]")
    elif test_cases_passed == 4:
        points = 20
        print(f"\n  ~ MOSTLY CORRECT [20/25 points]")
    elif test_cases_passed == 3:
        points = 15
        print(f"\n  ~ PARTIAL [15/25 points]")
    elif test_cases_passed == 2:
        points = 10
        print(f"\n  ~ MINIMAL [10/25 points]")
    elif test_cases_passed == 1:
        points = 5
        print(f"\n  ~ VERY MINIMAL [5/25 points]")
    else:
        points = 0
        print(f"\n  ✗ ALL TESTS FAILED [0/25 points]")

    print(f"\n{'─' * 60}")
    print(f"Part 2 Score: {points}/{total_points} points ({test_cases_passed}/{total_test_cases} tests passed)")
    print(f"{'─' * 60}\n")
    return points


def test_bfs_comprehensive():
    """
    Comprehensive BFS tests with multiple test cases
    to prevent hardcoded answers
    """

    print("=" * 60)
    print("PART 3: BFS TESTS (GRADER VERSION)")
    print("=" * 60)

    points = 0
    total_points = 25
    test_cases_passed = 0
    total_test_cases = 5

    try:
        adjacency = read_bfs_graph()
    except Exception as e:
        print(f"  ✗ FAIL: Could not read the graph for BFS validation: {e}")
        return 0

    cases = [(159, 212), (159, 167), (3, 240), (159, 200), (200, 81)]
    for case_number, (start_node, last_node) in enumerate(cases, 1):
        print(f"\n[Test {case_number}/5] BFS from node {start_node} to node {last_node}")
        conn = None
        try:
            conn = interface.Interface("neo4j://localhost:7687", "neo4j", "graphprocessing")
            with suppress_neo4j_notifications():
                result = conn.bfs(start_node, last_node)
            valid, reason = validate_bfs_result(result, start_node, last_node, adjacency)
            if valid:
                print("  ✓ PASS")
                test_cases_passed += 1
            else:
                print(f"  ✗ FAIL: {reason}")
        except Exception as e:
            print(f"  ✗ FAIL: {e}")
        finally:
            close_interface(conn)

    # Calculate points based on test cases passed
    if test_cases_passed == 5:
        points = 25
        print(f"\n  ✓ ALL TESTS PASSED [25/25 points]")
    elif test_cases_passed == 4:
        points = 20
        print(f"\n  ~ MOSTLY CORRECT [20/25 points]")
    elif test_cases_passed == 3:
        points = 15
        print(f"\n  ~ PARTIAL [15/25 points]")
    elif test_cases_passed == 2:
        points = 10
        print(f"\n  ~ MINIMAL [10/25 points]")
    elif test_cases_passed == 1:
        points = 5
        print(f"\n  ~ VERY MINIMAL [5/25 points]")
    else:
        points = 0
        print(f"\n  ✗ ALL TESTS FAILED [0/25 points]")

    print(f"\n{'─' * 60}")
    print(f"Part 3 Score: {points}/{total_points} points ({test_cases_passed}/{total_test_cases} tests passed)")
    print(f"{'─' * 60}\n")
    return points


def main():

    print("\n" + "=" * 60)
    print("PROJECT 1: GRADER VERSION - COMPREHENSIVE TESTS")
    print("=" * 60)
    print("\n⚠️  WARNING: This is the grader version with multiple test cases")
    print("   to prevent hardcoded solutions.\n")
    print("Connecting to Neo4j server", end="")
    sys.stdout.flush()

    if not wait_for_server():
        return 1

    # Initialize scores
    total_score = 0

    # Test load data
    tester = TesterConnect("neo4j://localhost:7687", "neo4j", "graphprocessing")
    try:
        score_part1 = tester.test_data_loaded()
        total_score += score_part1
    finally:
        tester.close()

    # Test PageRank (comprehensive)
    score_part2 = test_page_rank_comprehensive()
    total_score += score_part2

    # Test BFS (comprehensive)
    score_part3 = test_bfs_comprehensive()
    total_score += score_part3

    # Final Summary
    print("\n" + "=" * 60)
    print("FINAL SCORE SUMMARY (GRADER VERSION)")
    print("=" * 60)
    print(f"\nPart 1 - Data Loading:     {score_part1}/10 points")
    print(f"Part 2 - PageRank:         {score_part2}/25 points (5 test cases)")
    print(f"Part 3 - BFS:              {score_part3}/25 points (5 test cases)")
    print(f"\n{'─' * 60}")
    print(f"TOTAL SCORE:               {total_score}/60 points")
    print(f"{'─' * 60}")
    print(f"\nNote: Dockerfile configuration worth 40 points is graded separately")
    print("      Total project points: 100")
    print("\n" + "=" * 60 + "\n")


if __name__ == "__main__":
    raise SystemExit(main() or 0)
