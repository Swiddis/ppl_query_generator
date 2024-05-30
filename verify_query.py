import json
from opensearchpy import OpenSearch
import sys

def run_ppl_query(client: OpenSearch, ppl_query: str) -> dict:
    """
    Send a ppl query to the OpenSearch cluster.


    Parameters
    ----------
    client: OpenSearch
                The OpenSearch client.
    ppl_query: str
                The PPL query.


    Returns
    -------
    result: str
                The result.
    """
    request = "/_plugins/_ppl"
    query = json.dumps({"query": ppl_query})

    result = {}
    try:
        result["response"] = client.transport.perform_request(
            "POST", request, body=query
        )
        result["success"] = 1
        result["error"] = None
    except Exception as re:
        result["response"] = None
        result["success"] = 0
        result["status_code"] = re.status_code
        result["error"] = re.error
    return result

def verify(client: OpenSearch, ppl_query: str):
    result = run_ppl_query(client, ppl_query)
    if result["success"] == 1:
        print(ppl_query)
    else:
        print(f"Encountered error:\n> Query: {ppl_query}\n> Error: {result['error']}", file=sys.stderr)

def make_client():
    try:
        with open("client_conf.json", "r") as conf_file:
            conf = json.load(conf_file)
    except FileNotFoundError:
        print("No config found.\nThere must be a file `client_conf.json` with `host`, `user`, and `pass` keys.")
        sys.exit(1)
    
    host = conf["host"]
    port = 443
    auth = (conf["user"], conf["pass"])

    return OpenSearch(
        hosts = [{'host': host, 'port': port}],
        http_compress = True, 
        http_auth = auth,
        use_ssl = True,
        verify_certs = False,
        ssl_assert_hostname = False,
        ssl_show_warn = False,
    )
