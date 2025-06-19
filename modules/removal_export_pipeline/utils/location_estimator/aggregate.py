import numpy as np
from location_estimator.latlng import LatLng


def aggregate(objects, min_distance_between_shops: int = 8):
    # min dist between stores
    threshold_2 = min_distance_between_shops

    # radius for calculating density
    threshold_1 = threshold_2 / 2

    num = len(objects)
    px = np.zeros(num)
    py = np.zeros(num)
    rho = np.zeros(num)
    alat = np.zeros(num)
    alng = np.zeros(num)
    label = [[objects[i].label] for i in range(num)]
    image = [[objects[i].image] for i in range(num)]
    ann_id = [[objects[i].id] for i in range(num)]

    if num < 1:
        return []

    for i in range(1, num):
        xy = objects[0].get_xy(objects[i])
        px[i] = xy.x
        py[i] = xy.y

    for i in range(num):
        s = 1
        ave_x = px[i]
        ave_y = py[i]
        for j in range(num):
            if (
                i != j
                and (px[i] - px[j]) ** 2 + (py[i] - py[j]) ** 2 < threshold_1**2
            ):
                s += 1
                ave_x += px[j]
                ave_y += py[j]
                label[i].append(objects[j].label)
                image[i].append(objects[j].image)
                ann_id[i].append(objects[j].id)
        rho[i] = s + np.random.rand() * 0.01
        ll = objects[0].get_latlng(ave_x / s, ave_y / s)
        alat[i] = ll.lat
        alng[i] = ll.lng

    dis = np.zeros(num)

    for i in range(num):
        m = 2147483647
        for j in range(num):
            if (rho[j] > rho[i]) and ((px[i] - px[j]) ** 2 + (py[i] - py[j]) ** 2 < m):
                m = (px[i] - px[j]) ** 2 + (py[i] - py[j]) ** 2
        dis[i] = m

    aggregated_objects = []
    for i in range(num):
        if dis[i] > threshold_2**2:
            ll = LatLng(alat[i], alng[i])
            ll.label = label[i]
            ll.image = image[i]
            ll.id = ann_id[i]
            aggregated_objects.append(ll)
    return aggregated_objects
