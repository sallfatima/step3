LIGHTGLUE_CONF = {
    "threshold": 12,
    "feature": {
        "output": "feats-superpoint-n4096-rmax1600",
        "model": {
            "name": "superpoint",
            "nms_radius": 3,
            "max_keypoints": 4000,
            "keypoint_threshold": 0,
        },
        "preprocessing": {
            "grayscale": True,
            "force_resize": False,
            "resize_max": 1600,
            "width": 640,
            "height": 480,
            "dfactor": 8,
        },
    },
    "matcher": {
        "output": "matches-lightglue",
        "model": {
            "name": "lightglue",
            "match_threshold": 0.2,
            "width_confidence": 0.99,
            "depth_confidence": 0.95,
            "features": "superpoint",
            "model_name": "superpoint_lightglue.pth",
            "max_keypoints": 4000,
        },
    },
    "ransac": {
        "max_iter": 20000,
        "enable": True,
        "estimator": "poselib",
        "geometry": "homography",
        "method": "CV2_USAC_MAGSAC",
        "reproj_threshold": 8,
        "confidence": 0.9999,
    },
    "dense": False,
}

DEDODE_CONF = {
    "threshold": 10,
    "feature": {
        "output": "feats-dedode-n5000-r1600",
        "model": {
            "name": "dedode",
            "max_keypoints": 4000,
            "keypoint_threshold": 0,
        },
        "preprocessing": {
            "grayscale": False,
            "force_resize": False,
            "resize_max": 1600,
            "width": 768,
            "height": 768,
            "dfactor": 8,
        },
    },
    "matcher": {
        "output": "matches-Dual-Softmax",
        "model": {
            "name": "dual_softmax",
            "match_threshold": 0.1,
            "inv_temperature": 20,
            "max_keypoints": 5000,
        },
    },
    "ransac": {
        "max_iter": 20000,
        "enable": True,
        "estimator": "poselib",
        "geometry": "homography",
        "method": "CV2_USAC_MAGSAC",
        "reproj_threshold": 8,
        "confidence": 0.9999,
    },
    "dense": False,
}

DARKFEAT_CONF = {
    "threshold": 85,
    "feature": {
        "output": "feats-darkfeat-n5000-r1600",
        "model": {
            "name": "darkfeat",
            "max_keypoints": 4000,
            "reliability_threshold": 0.7,
            "repetability_threshold": 0.7,
            "keypoint_threshold": 0,
        },
        "preprocessing": {
            "grayscale": False,
            "force_resize": True,
            "resize_max": 1600,
            "width": 640,
            "height": 480,
            "dfactor": 8,
        },
    },
    "matcher": {
        "output": "matches-NN-mutual",
        "model": {
            "name": "nearest_neighbor",
            "do_mutual_check": True,
            "match_threshold": 0.3,
            "max_keypoints": 4000,
        },
    },
    "ransac": {
        "max_iter": 20000,
        "enable": True,
        "estimator": "poselib",
        "geometry": "homography",
        "method": "CV2_USAC_MAGSAC",
        "reproj_threshold": 8,
        "confidence": 0.9999,
    },
    "dense": False,
}
