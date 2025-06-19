import matplotlib.pyplot as plt
from location_estimator.buildingmanager import BuildingManager
from location_estimator.estimator import Raycast
from location_estimator.latlng import LatLng


class Annotation:
    def __init__(self, image_id, label_id, top, left, bottom, right):
        self.image_id = image_id
        self.label_id = label_id
        self.top = min(top, bottom)
        self.left = min(left, right)
        self.bottom = max(top, bottom)
        self.right = max(left, right)
        self.raycast = None


class Image:
    def __init__(
        self, name, x, y, pitch, heading, camera_height, fov, annotations, width, height
    ):
        self.name = name
        self.lng = x
        self.lat = y
        self.pitch = pitch
        self.heading = heading
        self.camera_height = camera_height
        self.fov = fov
        self.n_annotations = annotations
        self.annotations = []
        self.width = width
        self.height = height

    def append_annotation(self, annotation):
        annotation.raycast = Raycast(
            LatLng(self.lat, self.lng),
            self.width,
            self.height,
            (annotation.left + annotation.right) / 2,
            annotation.top,
            self.heading,
            self.height,
            self.pitch,
            self.fov,
        )
        self.annotations.append(annotation)


class City:
    def __init__(self, map_file, bbox: tuple = None):
        self.buildings = BuildingManager(map_file, bbox)

    def locate(self, annotation):
        i = annotation.raycast.intersection(self.buildings)
        if i is None or i["latlng"] is None:
            return None
        return i["latlng"].lng, i["latlng"].lat

    def plot(self, annotation):
        plt.figure()
        i = self.locate(annotation)
        list_bb = self.buildings.find_buildings(annotation.raycast.camera_latlng)
        for bb in list_bb:
            ndx = [nd.lng for nd in bb.nodes]
            ndy = [nd.lat for nd in bb.nodes]
            plt.plot(ndx, ndy)
        if i is None:
            annotation.raycast.plot()
        else:
            plt.scatter(
                annotation.raycast.camera_latlng.lng,
                annotation.raycast.camera_latlng.lat,
            )
            plt.plot(
                [annotation.raycast.camera_latlng.lng, i[0]],
                [annotation.raycast.camera_latlng.lat, i[1]],
            )

        plt.show()
