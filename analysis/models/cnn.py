from dataclasses import dataclass
import torch
import torch.nn as nn
from typing import Literal


@dataclass
class Conv1DConfig:
    num_classes: int
    num_features: int
    symbol_embedding_dim: int
    # kernel size for convolution
    kernel_size: int = 3
    # number of convolutional blocks
    num_blocks: int = 3
    # number of hidde linear layers between CNN feature maps and final classification
    num_fc_layers: int = 1
    # dropout ratio for convolutional activations
    cnn_dropout: float = 0.1
    # dropout ratio for MLP layers at the end of the network
    mlp_dropout: float = 0.3
    # number of channels used internally by convolutional layers
    cnn_width: int = 64
    # number of activations in hidden linear layers
    mlp_width: int = 128
    activation_fn: nn.Module = nn.ReLU()
    # technique for flattening CNN feature maps to a linear layer:
    # "global_avg" averages each channel over time, "append" concatenates
    # every timestep's activations into a single vector
    # flattening: Literal["global_avg", "append"] = "global_avg"
    # input sequence length, required when flattening == "append" since it
    # determines the size of the flattened feature vector
    # seq_len: int | None = None


class ConvBlock(nn.Module):
    def __init__(
        self,
        c_in,
        c_out,
        k,
        dropout,
        activation_fn: nn.Module,
        dilation=1,
        residual=False,
    ):
        super().__init__()
        if residual and c_in != c_out:
            raise ValueError(
                f"residual connection requires c_in == c_out, got {c_in} != {c_out}"
            )
        self.residual = residual
        self.net = nn.Sequential(
            nn.Conv1d(c_in, c_out, k, padding="same", dilation=dilation),
            nn.BatchNorm1d(c_out),
            activation_fn,
            nn.Dropout(dropout),
        )

    def forward(self, x):
        out = self.net(x)
        return x + out if self.residual else out


class Conv1DClassifier(nn.Module):
    def __init__(self, config: Conv1DConfig):
        super().__init__()

        cnn_layers = [
            ConvBlock(
                config.num_features,
                config.cnn_width,
                k=config.kernel_size,
                dropout=config.cnn_dropout,
                activation_fn=config.activation_fn,
                dilation=1,
                residual=False,
            )
        ]
        for i in range(1, config.num_blocks):
            cnn_layers.append(
                ConvBlock(
                    c_in=config.cnn_width,
                    c_out=config.cnn_width,
                    k=config.kernel_size,
                    dropout=config.cnn_dropout,
                    activation_fn=config.activation_fn,
                    dilation=2**i,
                    residual=True,
                )
            )
        self.blocks = nn.Sequential(*cnn_layers)

        self.pool = nn.AdaptiveAvgPool1d(1)

        fc_layers: list[nn.Module] = [nn.Flatten()]
        in_features = config.cnn_width + config.symbol_embedding_dim
        for _ in range(config.num_fc_layers):
            fc_layers += [
                nn.Linear(in_features, config.mlp_width),
                config.activation_fn,
                nn.Dropout(config.mlp_dropout),
            ]
            in_features = config.mlp_width
        fc_layers.append(nn.Linear(in_features, config.num_classes))
        self.head = nn.Sequential(*fc_layers)

    def forward(self, x, symbol):  # x: (N, C, L), symbol: (N,) long
        features = self.pool(self.blocks(x)).flatten(1)
        return self.head(torch.cat([features, symbol], dim=1))
