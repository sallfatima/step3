from dataclasses import replace
from typing import Dict, List, Optional, Tuple

from graph.graph import Graph
from graph.graph_types import Edge, EdgeData, Vertex, VertexData
from osm.osm_types import OSMNode, OSMWay

from ..utils import geo_tools


# @timer.timer
def build_graph_from_osm(
    nodes: Dict[int, OSMNode],
    ways: List[OSMWay],
    enrich: bool = False,
    distance_between_points: int = 0,
) -> Graph:
    g = Graph()

    # 1. create mapping to 0 based index nodes
    node_ids = nodes.keys()
    id_mapper = dict(zip(node_ids, range(len(node_ids))))

    # 2. add nodes and edges
    _add_nodes(g, id_mapper, nodes)

    if enrich:
        # This funtion already adds edges to enriched graph
        _enrich_graph(g, id_mapper, ways, distance_between_points)
    else:
        _add_edges(g, id_mapper, ways)

    return g


def _add_nodes(g: Graph, id_mapper: Dict[int, int], nodes: Dict[int, OSMNode]) -> None:
    for n in nodes.values():
        g.add_node(Vertex(id_mapper[n.osm_id], data=VertexData(n.lat, n.lon)))


def _add_generated_nodes(
    g: Graph, id_mapper: Dict[int, int], gen_nodes: List[Optional[Tuple[float, float]]]
) -> Tuple[Dict[int, int], List[int]]:
    new_nodes_ids = []
    for n in gen_nodes:
        gen_node_id = max(list(id_mapper.values())) + 1
        id_mapper[-abs(gen_node_id)] = gen_node_id
        g.add_node(Vertex(gen_node_id, data=VertexData(n[0], n[1])))
        new_nodes_ids.append(gen_node_id)
    return id_mapper, new_nodes_ids


def _generate_points(
    lat1: float, lon1: float, lat2: float, lon2: float, distance_between_points=0.05
):
    """
    Generate points along a straight line between two given points with a specified
    distance between them.
    """
    # Calculate the initial distance in meters between the two points
    total_distance = geo_tools.distance(lat1, lon1, lat2, lon2)

    # Calculate the number of points to generate
    num_points = int(total_distance / distance_between_points)

    if num_points == 0:
        return []
    # Calculate the increment in latitude and longitude for each point
    delta_lat = (lat2 - lat1) / num_points
    delta_lon = (lon2 - lon1) / num_points

    # Generate the points
    points = [
        (lat1 + i * delta_lat, lon1 + i * delta_lon) for i in range(1, num_points)
    ]

    return points


def _enrich_graph(
    g: Graph,
    id_mapper: Dict[int, int],
    ways: List[OSMWay],
    distance_between_points: int = 0,
) -> None:
    bidirectional_edges: Dict[Tuple[int, int], int] = {}
    for w in ways:
        for i in range(len(w.nodes) - 1):
            s_id, t_id = id_mapper[w.nodes[i]], id_mapper[w.nodes[i + 1]]
            s, t = g.vertices[s_id], g.vertices[t_id]

            generated_points = _generate_points(
                s.data.lat, s.data.lon, t.data.lat, t.data.lon, distance_between_points
            )
            id_mapper, new_nodes_ids = _add_generated_nodes(
                g, id_mapper, generated_points
            )

            all_points = (
                [(s.data.lat, s.data.lon)]
                + generated_points
                + [(t.data.lat, t.data.lon)]
            )
            all_ids = [s_id] + new_nodes_ids + [t_id]
            for j in range(len(all_points) - 1):
                length = round(
                    geo_tools.distance(
                        all_points[j][0],
                        all_points[j][1],
                        all_points[j + 1][0],
                        all_points[j + 1][1],
                    ),
                    2,
                )
                data = EdgeData(
                    length=length,
                    highway=w.highway,
                    max_v=w.max_speed_int,
                    name=w.name,
                    osm_id=w.osm_id,
                )
                start_id = all_ids[j]
                stop_id = all_ids[j + 1]
                edge = Edge(start_id, stop_id, w.forward, w.backward, data=data)

                if w.forward and w.backward:
                    smaller, bigger = min(start_id, stop_id), max(start_id, stop_id)
                    if (smaller, bigger) in bidirectional_edges:
                        print(
                            f"found duplicated bidirectional edge {(smaller, bigger)}"
                        )
                        print(
                            f"(osm ids {w.osm_id} and {bidirectional_edges[(smaller, bigger)]})... skipping one"
                        )
                        continue
                    bidirectional_edges[(smaller, bigger)] = w.osm_id

                g.add_edge(edge)


def _add_edges(g: Graph, id_mapper: Dict[int, int], ways: List[OSMWay]) -> None:
    bidirectional_edges: Dict[Tuple[int, int], int] = {}
    for w in ways:
        for i in range(len(w.nodes) - 1):
            s_id, t_id = id_mapper[w.nodes[i]], id_mapper[w.nodes[i + 1]]
            s, t = g.vertices[s_id], g.vertices[t_id]
            length = round(
                geo_tools.distance(s.data.lat, s.data.lon, t.data.lat, t.data.lon), 2
            )
            data = EdgeData(
                length=length, highway=w.highway, max_v=w.max_speed_int, name=w.name
            )
            edge = Edge(s_id, t_id, w.forward, w.backward, data=data)
            if w.forward and w.backward:
                smaller, bigger = min(s_id, t_id), max(s_id, t_id)
                if (smaller, bigger) in bidirectional_edges:
                    print(f"found duplicated bidirectional edge {(smaller, bigger)}")
                    print(
                        f"(osm ids {w.osm_id} and {bidirectional_edges[(smaller, bigger)]})... skipping one"
                    )
                    continue
                bidirectional_edges[(smaller, bigger)] = w.osm_id

            g.add_edge(edge)


# @timer.timer
def build_graph_from_vertices_edges(vertices: List[Vertex], edges: List[Edge]) -> Graph:
    g = Graph()

    # 1. add all nodes and create mapping to 0 based index nodes
    vertex_ids = set(v.id for v in vertices)
    id_mapper = dict(zip(vertex_ids, range(len(vertex_ids))))
    for v in vertices:
        g.add_node(Vertex(id_mapper[v.id], v.data))

    # 2. create edges with proper node ids
    valid_edges = [e for e in edges if e.s in vertex_ids and e.t in vertex_ids]
    new_edges = [replace(e, s=id_mapper[e.s], t=id_mapper[e.t]) for e in valid_edges]

    # 3. add those new edges to the graph
    for e in new_edges:
        g.add_edge(e)

    return g
