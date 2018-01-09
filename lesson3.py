from fastai.structured import *
from fastai.column_data import *
np.set_printoptions(threshold=50, edgeitems=50)

PATH = "/Users/edwizzle/Developer/fastai_v2/data/rossmann/"

def concat_csvs(dirname):
    path = f'{PATH}{dirname}'
    filenames = glob(f"{path}/*.csv")

    wrote_header = False
    with open(f"{path}.csv", "w") as outputfile:
        for filename in filenames:
            name = filename.split(".")[0]
            with open(filename) as f:
                line = f.readline()
                if not wrote_header:
                    wrote_header = True
                    outputfile.write("file," + line)
                for line in f:
                    outputfile.write(name + "," + line)
                outputfile.write("\n")


# concat_csvs('googletrend')
# concat_csvs('weather')


table_names = ['train', 'store', 'store_states', 'state_names', 'googletrend', 'weather', 'test']

tables = [pd.read_csv(f'{PATH}{fname}.csv', low_memory=False) for fname in table_names]

from IPython.display import HTML

for t in tables: display(t.head())

for t in tables: display(DataFrameSummary(t).summary())

train, store, store_states, state_names, googletrend, weather, test = tables