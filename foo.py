import numpy as np
import pandas as pd

# generate some test data
n = 975
df = pd.DataFrame({
    'x': np.arange(0, n),
    'y': np.random.normal(size=n),
})
print(f'Starting length: {len(df)}')

# split data into chunks
chunk_size = 10000
chunks = []
num_chunks = int(np.ceil(n / chunk_size))
for i in range(num_chunks):
    start = chunk_size * i
    stop = start + chunk_size
    chunks.append(df[start:stop])

# iterate over chunks    
for i in range(num_chunks):
    print(f'Chunk {i}: {len(chunks[i])}')
