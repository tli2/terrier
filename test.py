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

  def train(self, pandas_df):
    dataset = arrow_io.ArrowDataset.from_pandas(pandas_df)
    dataset = dataset.shuffle(buffer_size=1024).batch(8)
    self.model.fit(dataset)

  def eval(self, pandas_df):
    print("Evaluating...\n")
    model.evaluate(arrow_io.ArrowDataset.from_pandas(pandas_df).batch(8))

network = StupidNetwork()
t = client.read_table("snode", 0.0)
network.train(t.to_pandas())
network.eval(t.to_pandas())
