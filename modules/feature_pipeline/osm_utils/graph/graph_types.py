from dataclasses import dataclass


@dataclass(frozen=True)
class VertexData:
    __slots__ = ["lat", "lon"]
    lat: float
    lon: float

    def __repr__(self) -> str:
        return f"{self.lat} {self.lon}"


@dataclass(frozen=True)
class Vertex:
    __slots__ = ["id", "data"]
    id: int
    data: VertexData

    @property
    def description(self) -> str:
        return f"{self.id} {self.data}"


@dataclass(frozen=True)
class EdgeData:
    __slots__ = ["length", "highway", "max_v", "name", "osm_id"]
    length: float
    highway: str
    max_v: int
    name: str
    osm_id: int

    def __repr__(self) -> str:
        return f"{self.length} {self.highway} {self.max_v} {self.name} {self.osm_id}"


@dataclass(frozen=True)
class Edge:
    __slots__ = ["s", "t", "forward", "backward", "data"]
    s: int
    t: int
    forward: bool
    backward: bool
    data: EdgeData

    @property
    def description(self) -> str:
        both_directions = "1" if self.forward and self.backward else "0"
        return f"{self.s} {self.t} {self.data} {both_directions}"
