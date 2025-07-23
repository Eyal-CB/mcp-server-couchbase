from datetime import timedelta
from typing import Any
from mcp.server.fastmcp import FastMCP, Context
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator, CertificateAuthenticator
from couchbase.options import ClusterOptions
import logging
from dataclasses import dataclass
from contextlib import asynccontextmanager
from typing import AsyncIterator
import click
from couchbase_mcp import logger, set_mcp, MCP_SERVER_NAME
import json
import importlib
import os
import requests
from requests.auth import HTTPBasicAuth


SERVICE_PORT_MAPPING = {
    "tls" : {
        "admin" : "18091",
        "index" : "19012",
        "query" : "18093",
        "fts" : "18094",
        "cbas" : "18095",
        "eventing" : "18097"

    },
    "no_tls" : {
        "admin" : "8091",
        "index" : "9012",
        "query" : "8093",
        "fts" : "8094",
        "cbas" : "8095",
        "eventing" : "8097"

    }
}

# Configure logging


@dataclass
class AppContext:
    """Context for the MCP server."""

    cluster: Cluster | None = None
    read_only_query_mode: bool = True
    username: str = None
    password: str = None
    connection_string: str = None
    ca_cert_path : str = None
    use_tls : bool = True
    client_cert_path : str = None


def validate_required_param(
    ctx: click.Context, param: click.Parameter, value: str | None
) -> str:
    """Validate that a required parameter is not empty."""
    if not value or value.strip() == "":
        raise click.BadParameter(f"{param.name} cannot be empty")
    return value

def validate_authentication_method(params : dict ) -> bool:
    """Util function to verify either user/password combination OR client certificates have been included"""
    username = params.get("username")
    password = params.get("password")
    client_cert_path = params.get("client_cert_path")
    ca_cert_path = params.get("ca_cert_path")

    # Strip values to check for empty strings
    if username is not None:
        username = username.strip()
    if password is not None:
        password = password.strip()

    if client_cert_path:
        client_cert = os.path.join(client_cert_path, "client.pem")
        client_key = os.path.join(client_cert_path, "client.key")

        if not os.path.isfile(client_cert) or not os.path.isfile(client_key):
            raise click.BadParameter(
                f"Client certificate files not found in {client_cert_path}. Required: client.pem and client.key."
            )

        if username or password or username == "" or password =="":
            raise click.BadParameter(
                "You must use either a client certificate or username/password, not both."
            )

    elif username or password:
        if not username or not password:
            raise click.BadParameter(
                "Both username and password must be provided and non-empty if using basic authentication."
            )
    else:
        raise click.BadParameter(
            "You must provide either a client certificate path or username/password combination, neither received."
        )

    if not ca_cert_path:
        logger.warning(f"A trusted CA certificate has not been provided, using local trust store for TLS connections")



def get_settings() -> dict:
    """Get settings from Click context."""
    ctx = click.get_current_context()
    return ctx.obj or {}


@click.command()
@click.option(
    "--connection-string",
    envvar="CB_CONNECTION_STRING",
    help="Couchbase connection string",
    callback=validate_required_param,
)
@click.option(
    "--username",
    envvar="CB_USERNAME",
    help="Couchbase database user",
    #callback=validate_required_param,
)
@click.option(
    "--password",
    envvar="CB_PASSWORD",
    help="Couchbase database password",
    #callback=validate_required_param,
)

@click.option(
    '--ca-cert-path',
    envvar="CA_CERT_PATH",
    type=click.Path(exists=True),
    default=None,
    help='Path to Server TLS certificate, uses local trust store if missing')

@click.option(
    "--client-cert-path",
    envvar="CLIENT_CERT_PATH",
    default=None,
    help="Path to client.key and client.pem files for mtls client authentication",
    #callback=validate_required_param,
)

@click.option(
    '--ca-cert-path',
    envvar="CA_CERT_PATH",
    type=click.Path(exists=True),
    default=None,
    help='Path to Server TLS certificate, required for API calls.')

@click.option(
    "--read-only-query-mode",
    envvar="READ_ONLY_QUERY_MODE",
    type=bool,
    default=True,
    help="Enable read-only query mode. Set to True (default) to allow only read-only queries. Can be set to False to allow data modification queries.",
)
@click.option(
    "--transport",
    envvar="MCP_TRANSPORT",
    type=click.Choice(["stdio", "sse"]),
    default="stdio",
    help="Transport mode for the server (stdio or sse)",
)
@click.pass_context
def main(
    ctx,
    connection_string,
    username,
    password,
    read_only_query_mode,
    ca_cert_path,
    client_cert_path,
    transport,
):
    """Couchbase MCP Server"""
    ctx.obj = {
        "connection_string": connection_string,
        "username": username,
        "password": password,
        "read_only_query_mode": read_only_query_mode,
        "ca_cert_path" : ca_cert_path,
        "client_cert_path" : client_cert_path
    }
    try:
        validate_authentication_method(ctx.obj)
    except Exception as e:
        logger.error(f"Failed to validate auth method params: {e}")
        raise 
    mcp.run(transport=transport)


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """Initialize the Couchbase cluster and bucket for the MCP server."""
    # Get configuration from Click context
    settings = get_settings()

    connection_string = settings.get("connection_string")
    username = settings.get("username")
    password = settings.get("password")
    read_only_query_mode = settings.get("read_only_query_mode")
    ca_cert_path = settings.get("ca_cert_path")
    client_cert_path = settings.get("client_cert_path")
    use_tls = True
    if "://" in connection_string:
        protocol = connection_string.split("://", 1)[0]
        use_tls = (protocol[-1] == 's')


    try:
        logger.info("Creating Couchbase cluster connection...")
<<<<<<< HEAD
        #use client cert if provided, else user/password
        if client_cert_path:
                    
            tls_conf = {
                "cert_path" :  os.path.join(client_cert_path, "client.pem"),
                "key_path" :  os.path.join(client_cert_path, "client.key"),
            }
            #set ca cert as trust store if provided
            if ca_cert_path:
                tls_conf["trust_store_path"] = ca_cert_path
            auth = CertificateAuthenticator(**tls_conf)
        else:
            auth = PasswordAuthenticator(username, password, cert_path = ca_cert_path)
=======
        auth = PasswordAuthenticator(username, password, cert_path = ca_cert_path)
>>>>>>> cluster_health

        options = ClusterOptions(auth)
        options.apply_profile("wan_development")

        cluster = Cluster(connection_string, options)
        cluster.wait_until_ready(timedelta(seconds=5))
        logger.info("Successfully connected to Couchbase cluster")

        yield AppContext(
            cluster=cluster, 
            ca_cert_path = ca_cert_path,
            connection_string = connection_string,
            username=username,
            password=password,
            read_only_query_mode=read_only_query_mode,
            use_tls=use_tls,
            client_cert_path=client_cert_path
        )

    except Exception as e:
        logger.error(f"Failed to connect to Couchbase: {e}")
        raise


# Initialize MCP server
mcp = FastMCP(MCP_SERVER_NAME, lifespan=app_lifespan)
set_mcp(mcp)

# Tools
@mcp.tool()
def get_cluster_health_check(
    ctx: Context) -> list[dict[str,Any]]:
    """Runs a healthcheck (ping report) on the Couchbase cluster. Returns an array of json objects with pintreport results and node IPs.
    Also useful for discovering available services in the cluster. Multiple services may reside on each node.
    Returns: Array of service objects, each having the URL (consisting of IP and port for the service) and state of the service"""
    cluster = ctx.request_context.lifespan_context.cluster

    services = []
    try:
        ping_report = cluster.ping()
    except Exception as e:
        logger.error(f"Unable to reach cluster for health check: {e}")
        raise e
    try:
        services = []
        for service_type, endpoints in ping_report.endpoints.items():
            for ep in endpoints:
                services.append({
                    "service": service_type.value,
                    "state": ep.state.value,
                    "id": ep.id,
                    "ip": ep.remote,
                    "latency_ms": ep.latency.total_seconds() * 1000 if ep.latency else None,
                    "error": str(ep.error) if ep.error else None
                })
    except Exception as e:
        logger.error(f"Unable to parse ping report: {e}")
        raise e

    return services

def call_api(ctx: Context, url: str ) -> requests.Response:
    """Call Couchbase API for given URL. Returns raw Response object. Retrieve auth values from App Context.
    - If `client_cert_path` is not None, it uses 'client.pem' and 'client.key' in that directory.
    - If `client_cert_path` is None, it uses basic auth with `username` and `password`.
    """

    username = ctx.request_context.lifespan_context.username
    password = ctx.request_context.lifespan_context.password
    ca_cert_path = ctx.request_context.lifespan_context.ca_cert_path or True
    client_cert_path = ctx.request_context.lifespan_context.client_cert_path
    auth=None
    cert=None

    if client_cert_path:
        # mTLS mode
        try:
            client_cert = os.path.join(client_cert_path, "client.pem")
            client_key = os.path.join(client_cert_path, "client.key")
            cert = (client_cert, client_key)
        except Exception as e:
            logger.error(f"unable to create client certificate object - {e}")
            raise

    else:
        if not username or not password:
            raise ValueError("Username and password must be provided for basic authentication.")
        auth = HTTPBasicAuth(username, password)
    try:
        response = requests.get(
            url,
            auth=auth,
            cert=cert,
            verify=ca_cert_path,  
            timeout=10
        )
        response.raise_for_status()
        return response
    except requests.exceptions.SSLError as e:
        logger.error(f"SSL verification failed: {e}")
        raise e
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching api: {e}")
        raise e
    except Exception as e:
        logger.error(f"Error fetching api: {e}")
        raise e

def build_api_url(ctx: Context, hostname: str, service: str, endpoint: str) -> str:
    """Return full url for an api call to given hostname, service and endpoint"""
    protocol = "http"
    port = SERVICE_PORT_MAPPING["no_tls"].get(service) or "8091"
    if( ctx.request_context.lifespan_context.use_tls):
        protocol = "https"
        port = SERVICE_PORT_MAPPING["tls"].get(service) or "18091"
    return f'{protocol}://{hostname}:{port}/{endpoint}'


def strip_protocol(url: str) -> str:
    parts = url.split("://", 1)
    return parts[1] if len(parts) == 2 else url

 
@mcp.tool()
def get_cluster_prometheus_metrics_endpoints(
    ctx: Context) -> list[str]:
    """Runs an API call to the Ccouchbase Prometheus Endpoint Service Discovery. Retrieves the hostnames and ports of all metrics endpoints for the cluster nodes.
    Required for calling the get_cluster_metrics function in a cluster.
    Accepts the hostname of the node to get metrics from.
    Returns: Array of hostname+port as strings"""

    hostname = strip_protocol(ctx.request_context.lifespan_context.connection_string)
    url =  build_api_url(ctx, hostname, "admin", "prometheus_sd_config" )
    try:
        response = call_api(ctx, url)
    except Exception as e:
        logger.error(f"Error calling API to fetch endpoints: {e}")
        raise e
    json_data = response.json()
    if (json_data and isinstance(json_data,list)):
        return json_data[0].get("targets")
    else:
        logger.error(f"Unexpected response from fetch endpoints API {response.text}")
        raise TypeError(f"Unexpected response from fetch endpoints API {response.text}")

@mcp.tool()
def get_cluster_node_metrics(
    ctx: Context, hostname: str) -> str:
    """Runs an API call to the metrics endpoint of a given couchbase node hostname. 
    Metrics contain info on nodes performance, resources and services.
    Accepts the node hostname as a string. 
    Returns: String representing the prometheus formatted metrics returned by couchbase."""

    hostname = strip_protocol(hostname)
    parts = hostname.split(":",1)
    hostname = parts[0]
    url = build_api_url(ctx, hostname, "admin", "metrics" )
    try:
        response = call_api(ctx, url)
        return response.text
    except Exception as e:
        logger.error(f"Error calling API to fetch metrics: {e}")
        raise e
    
def load_config(path="mcp_config.json"):

    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(SCRIPT_DIR, path)
    try:
        with open(file_path, "r") as f:
            config = json.load(f)
            return config
    except FileNotFoundError as e:
        logger.error(f"Config file not found: {file_path}")
        raise e
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in config file: {e}")
        raise e

def load_tool_modules(tools):
    for mod_name in tools:
        try:
            importlib.import_module(mod_name)
            logger.info(f"Loaded tool module: {mod_name}")
        except ModuleNotFoundError as e:
            logger.warning(f"Could not find tool module '{mod_name}': {e}")
        except Exception as e:
            logger.error(f"Error importing tool module '{mod_name}': {e}")



#Slow Query tools

@mcp.tool()
def advanced_aggregate_slow_queries_stats_by_pattern(ctx: Context, query_limit:int = 10) -> list[dict[str, Any]]:
    """Run an in depth analysis query on the completed_requests system catalog to discover potential slow running queries along with index scan and fetch counts and times. 
    This query attempts to reduce the statements of logged queries into patterns by removing any values and only looking at the query structure with regard to returned fields,
    filtered predicates, sorts and aggregations. 
    For each query pattern it returns execution durations, counts and example queries.
    
    Accepts an optional integer as a limit to the number of results returned (default 10).
    Returns an object array of query patterns with:
    1. total count and count per user running the query.
    2. min, max and average duration of the full execution.
    3. min, max and average duration of each of the sub-operations index scan (or primary scan) and Fetch.
    4. Count of number of documents found in index scans and the total fetched.
    5. An example instance of one of the queries run matching this pattern.
    """

    query_template = """
    SELECT grouped.query_pattern,
       grouped.statement_example,
       (OBJECT u: ARRAY_LENGTH(ARRAY v FOR v IN grouped.users_agg WHEN v = u END) FOR u IN ARRAY_DISTINCT(grouped.users_agg) END) AS user_query_counts,
       grouped.total_count,
       ROUND(grouped.min_duration_in_seconds, 3) AS min_duration_in_seconds,
       ROUND(grouped.max_duration_in_seconds, 3) AS max_duration_in_seconds,
       ROUND(grouped.avg_duration_in_seconds, 3) AS avg_duration_in_seconds,
       ROUND((grouped.sorted_durations[FLOOR(grouped.total_count / 2)] + 
              grouped.sorted_durations[CEIL(grouped.total_count / 2) - 1]) / 2, 3) AS median_duration_in_seconds,
       ROUND(grouped.avg_fetch_count, 3) AS avg_fetch_docs_count,
       ROUND(grouped.avg_primaryScan_count, 3) AS avg_primaryScan_docs_count,
       ROUND(grouped.avg_indexScan_count, 3) AS avg_indexScan_docs_count,
       ROUND(grouped.avg_fetch_time, 3) AS avg_fetch_duration_in_seconds,
       ROUND(grouped.avg_primaryScan_time, 3) AS avg_primaryScan_duration_in_seconds,
       ROUND(grouped.avg_indexScan_time, 3) AS avg_indexScan_duration_in_seconds
FROM (
    SELECT query_pattern,
           ARRAY_AGG(sub.users) AS users_agg,
           ARRAY_AGG(sub.statement)[0] as statement_example, 
           COUNT(*) AS total_count,
           MIN(duration_in_seconds) AS min_duration_in_seconds,
           MAX(duration_in_seconds) AS max_duration_in_seconds,
           AVG(duration_in_seconds) AS avg_duration_in_seconds,
           ARRAY_SORT(ARRAY_AGG(duration_in_seconds)) AS sorted_durations,
           AVG(sub.`fetch_count`) AS avg_fetch_count,
           AVG(sub.`primaryScan_count`) AS avg_primaryScan_count,
           AVG(sub.`indexScan_count`) AS avg_indexScan_count,
           AVG(sub.`fetch_time`) AS avg_fetch_duration_in_seconds,
           AVG(sub.`primaryScan_time`) AS avg_primaryScan_duration_in_seconds,
           AVG(sub.`indexScan_time`) AS avg_indexScan_duration_in_seconds           
    FROM (
        SELECT query_pattern,
               statement,
               users,
               STR_TO_DURATION(serviceTime) / 1000000000 AS duration_in_seconds,
               phaseCounts.`fetch` AS `fetch_count`,
               phaseCounts.`primaryScan` AS `primaryScan_count`,
               phaseCounts.`indexScan` AS `indexScan_count`,
               STR_TO_DURATION(phaseTimes.`fetch`)/ 1000000000 AS `fetch_duration_in_seconds`,
               STR_TO_DURATION(phaseTimes.`primaryScan`)/ 1000000000 AS `primaryScan_duration_in_seconds`,
               STR_TO_DURATION(phaseTimes.`indexScan`)/ 1000000000 AS `indexScan_duration_in_seconds`
        FROM system:completed_requests
        LET query_pattern = IFMISSING(preparedText, REGEX_REPLACE(
  REGEX_REPLACE(
  REGEX_REPLACE(
  REGEX_REPLACE(
  REGEX_REPLACE(
  REGEX_REPLACE(statement,
    "\\\\s+", " "),
    '"(?:[^"]|"")*"', "?"),
    "'(?:[^']|'')*'", "?"),
    "\\\\b-?\\\\d+\\\\.?\\\\d*\\\\b", "?"),
    "(?i)\\\\b(NULL|TRUE|FALSE)\\\\b", "?"),
    "(\\\\?\\\\s*,\\\\s*)+\\\\?", "?"))
        WHERE  UPPER(IFMISSING(preparedText, statement)) NOT LIKE 'INFER %'
              AND UPPER(IFMISSING(preparedText, statement)) NOT LIKE 'ADVISE %'
              AND UPPER(IFMISSING(preparedText, statement)) NOT LIKE 'CREATE %'
              AND UPPER(IFMISSING(preparedText, statement)) NOT LIKE 'CREATE INDEX%'
              AND UPPER(IFMISSING(preparedText, statement)) NOT LIKE 'ALTER INDEX%'
              AND UPPER(IFMISSING(preparedText, statement)) NOT LIKE '% SYSTEM:%'
    ) AS sub
    GROUP BY query_pattern
) AS grouped
ORDER BY grouped.total_count DESC
    LIMIT {limit}
    """

    query = query_template.format(limit=query_limit)
    try:
        result = system_catalog_query(ctx,query)
        return result
    except Exception as e:
        logger.error(f"Error completed_request query: {str(e)}", exc_info=True)
        raise e
    
@mcp.tool()
def retreive_single_slow_query_plan(ctx: Context, query_statement : str, query_limit:int = 10) -> list[dict[str, Any]]:
    """Retrieve the query execution report and execution plan of all executions of the given query saved in the completed_requests catalog.
    The query statement must be an exact match to the statement executed, including values or placeholders where applicable.
    Accepts a query statement and an optional integer as a limit to the number of results returned (default 10).
    Returns an object array of execution plans for all instances of the query, ordered by execution time from highest to lowest.
    """

    query_template = """
    SELECT r.*, meta(r).plan
    FROM system:completed_requests AS r
    WHERE UPPER(IFMISSING(preparedText, statement)) = UPPER('{query_statement}')
    ORDER BY STR_TO_DURATION(r.elapsedTime) DESC
    LIMIT {limit}
    """

    query = query_template.format(limit=query_limit,query_statement=query_statement)
    try:
        result = system_catalog_query(ctx,query)
        return result
    except Exception as e:
        logger.error(f"Error completed_request query: {str(e)}", exc_info=True)
        raise e
    
@mcp.tool()
def retreive_list_of_similar_queries_from_completed_requests_catalog(ctx: Context, query_statement : str, query_limit:int = 10) -> list[dict[str, Any]]:
    """Retrieve a list of all recorded query statements matching the pattern of the specified query (i.e. similar queries differing in predicate values and capitalizations).
    The query statement must be an exact match to the statement executed, including values or placeholders where applicable.
    Accepts a query statement and an optional integer as a limit to the number of results returned (default 10).
    Returns a list of query statements matching the pattern.
    """

    query_template = """
    SELECT raw r.statement
    FROM system:completed_requests AS r
    LET query_pattern = UPPER(IFMISSING(preparedText, REGEX_REPLACE(
    REGEX_REPLACE(
    REGEX_REPLACE(
    REGEX_REPLACE(
    REGEX_REPLACE(
    REGEX_REPLACE(statement,
        "\\\\s+", " "),
        '"(?:[^"]|"")*"', "?"),
        "'(?:[^']|'')*'", "?"),
        "\\\\b-?\\\\d+\\\\.?\\\\d*\\\\b", "?"),
        "(?i)\\\\b(NULL|TRUE|FALSE)\\\\b", "?"),
        "(\\\\?\\\\s*,\\\\s*)+\\\\?", "?")))
    WHERE query_pattern = REGEX_REPLACE(
    REGEX_REPLACE(
    REGEX_REPLACE(
    REGEX_REPLACE(
    REGEX_REPLACE(
    REGEX_REPLACE({query_statement},
        "\\\\s+", " "),
        '"(?:[^"]|"")*"', "?"),
        "'(?:[^']|'')*'", "?"),
        "\\\\b-?\\\\d+\\\\.?\\\\d*\\\\b", "?"),
        "(?i)\\\\b(NULL|TRUE|FALSE)\\\\b", "?"),
        "(\\\\?\\\\s*,\\\\s*)+\\\\?", "?")
    LIMIT {limit}
    """

    query = query_template.format(limit=query_limit,query_statement=query_statement)
    try:
        result = system_catalog_query(ctx,query)
        return result
    except Exception as e:
        logger.error(f"Error completed_request query: {str(e)}", exc_info=True)
        raise e
    
# Util Functions
def system_catalog_query(ctx: Context, query: str) -> list[dict[str, Any]]:
    cluster = ctx.request_context.lifespan_context.cluster
    try:

        results = []
        result = cluster.query(query)
        for row in result:
            results.append(row)
        return results
    except Exception as e:
        logger.error(f"Error running query: {str(e)}", exc_info=True)
        raise e



if __name__ == "__main__":
    importlib.import_module("tools") #load default tools
    config = load_config()
    tools = config.get("tools", [])
    load_tool_modules(tools)
    main()
