import torch
import torch.nn as nn
import torch.nn.functional as F
from DeDoDe.utils import get_grid

from .dinov2 import vit_large
from .layers.attention import MemEffAttention
from .layers.block import Block
