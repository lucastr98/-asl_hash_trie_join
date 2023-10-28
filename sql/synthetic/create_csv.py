import numpy as np
import pandas as pd
import math

#input
r = int(input("Input r, in {4, 5, 6, 7}: "))
if(r > 7 or r < 4):
    r = 7

d = int(input("Input d, in {1..10}: "))
if(d < 1 or d > 10):
    d = 1


R = np.arange(1, 10**7+1)
S = np.arange(1, math.floor((10**7 + 10**r)/2) + 1)
T = np.arange(math.floor((10**7 - 10**r)/2), 10**7)

R = np.repeat(R, d)
S = np.repeat(S, d)
T = np.repeat(T, d)

np.random.shuffle(R)
np.random.shuffle(S)
np.random.shuffle(T)

pd.DataFrame(R).to_csv("./data/R.csv", header=None, index=None)
pd.DataFrame(T).to_csv("./data/T.csv", header=None, index=None)
pd.DataFrame(S).to_csv("./data/S.csv", header=None, index=None)


