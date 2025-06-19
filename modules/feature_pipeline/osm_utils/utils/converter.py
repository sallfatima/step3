from .. import configuration as config
from ..graph import algorithms, contract_graph, convert_graph, graphfactory
from ..osm import read_osm, sanitize_input
from . import write_graph as output


# @timer.timer
def convert_osm_to_roadgraph(
    filename,
    network_type,
    unconnected_components,
    networkx_output,
    contract,
    enrich=False,
    distance_between_points: int = 0,
    bucket=None,
):
    configuration = config.Configuration(network_type)

    r_index = filename.rfind(".")
    out_file = filename[:r_index]

    cloud_decompression = None

    if bucket is not None:
        # Download the content of the blob as bytes
        cloud_blob = bucket.blob(filename)
        # Decode the bytes to string
        cloud_decompression = cloud_blob.download_as_string().decode("utf-8")

    nodes, ways = read_osm.read_file(
        filename, configuration, cloud_decompression=cloud_decompression
    )

    sanitize_input.sanitize_input(ways, nodes, verbose=False)

    graph = graphfactory.build_graph_from_osm(
        nodes, ways, enrich, distance_between_points
    )

    if not unconnected_components:
        graph = algorithms.computeLCCGraph(graph)

    # output.write_to_file(graph, out_file, configuration.get_file_extension())

    if networkx_output:
        nx_graph = convert_graph.convert_to_networkx(graph)
        output.write_nx_to_file(nx_graph, f"{out_file}.json", bucket)

    if contract:
        contracted_graph = contract_graph.ContractGraph(graph).contract()
        output.write_to_file(
            contracted_graph, out_file, f"{configuration.get_file_extension()}c"
        )
        if networkx_output:
            nx_graph = convert_graph.convert_to_networkx(contracted_graph)
            output.write_nx_to_file(nx_graph, f"{out_file}_contracted.json", bucket)
