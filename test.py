import tensorflow as tf
import numpy as np
import keras
from keras.models import Sequential
from keras import layers
from keras import optimizers
from keras import models
from keras import losses
from keras import backend as K
import tensorflow_io.arrow as arrow_io
import pyarrow
import client

class StupidNetwork:
  def __init__(self):
    self.model = Sequential()
    self.model.add(layers.Dense(20, input_dim=8))
    self.model.add(layers.Activation('relu'))
    self.model.add(layers.Dense(10))
    self.model.add(layers.Activation('relu'))
    self.model.add(layers.Dense(10))
    self.model.add(layers.Activation('relu'))
    self.model.add(layers.Dense(1))
    self.model.compile(loss=losses.mean_squared_error, optimizer=optimizers.Adam(lr=1e-4))

  def train(self, pandas_df, target):
    dataset = arrow_io.ArrowDataset.from_pandas(pandas_df)
    self.model.fit(dataset, target)

  def eval(self, pandas_df, target):
    print("Evaluating...\n")
    self.model.evaluate(arrow_io.ArrowDataset.from_pandas(pandas_df), target)

network = StupidNetwork()
t, d = client.read_table("snode", 0.0)
network.train(t.to_pandas(), d.to_numpy())
network.eval(t.to_pandas(), d.to_numpy())
