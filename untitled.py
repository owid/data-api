import os
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import seaborn as sns
import json
import sys
import datetime
from IPython import get_ipython

ipython = get_ipython()
ipython.magic("load_ext rich")
ipython.magic("load_ext autoreload")
ipython.magic("autoreload 2")
ipython.magic("matplotlib inline")
ipython.magic("config InlineBackend.figure_format = 'svg'")

# nice / large graphs
sns.set_context("notebook")
plt.rcParams["figure.figsize"] = (6, 3)